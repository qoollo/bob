use super::prelude::*;

#[derive(Clone, Debug)]
pub(crate) struct Group {
    holders: Arc<RwLock<Vec<Holder>>>,
    settings: Arc<Settings>,
    directory_path: PathBuf,
    vdisk_id: VDiskID,
    node_name: String,
    disk_name: String,
    owner_node_name: String,
    created_holder_indexes: Arc<RwLock<HashMap<u64, usize>>>,
    dump_sem: Arc<Semaphore>,
}

impl Group {
    pub fn new(
        settings: Arc<Settings>,
        vdisk_id: VDiskID,
        node_name: String,
        disk_name: String,
        directory_path: PathBuf,
        owner_node_name: String,
        dump_sem: Arc<Semaphore>,
    ) -> Self {
        Self {
            holders: Arc::new(RwLock::new(vec![])),
            settings,
            vdisk_id,
            node_name,
            directory_path,
            disk_name,
            owner_node_name,
            created_holder_indexes: Arc::default(),
            dump_sem,
        }
    }

    pub fn can_process_operation(&self, operation: &Operation) -> bool {
        trace!("check {} can process operation {:?}", self, operation);
        if operation.is_data_alien() {
            let name_matched = operation
                .remote_node_name()
                .map_or(true, |node_name| *node_name == self.node_name);
            name_matched && self.vdisk_id == operation.vdisk_id()
        } else {
            self.disk_name == operation.disk_name_local() && self.vdisk_id == operation.vdisk_id()
        }
    }

    pub async fn run(&self) -> Result<()> {
        debug!("{}: read holders from disk", self);
        let holders = self
            .settings
            .config()
            .try_multiple_times(
                || self.read_vdisk_directory(),
                "can't create pearl holders",
                self.settings.config().fail_retry_timeout(),
            )
            .await
            .with_context(|| "backend pearl group read vdisk directory failed")?;
        debug!("{}: count holders: {}", self, holders.len());
        if holders
            .iter()
            .all(|holder| holder.is_actual(self.settings.get_actual_timestamp_start()))
        {
            self.create_current_pearl();
        }
        debug!("{}: save holders to group", self);
        self.add_range(holders).await;
        debug!("{}: start holders", self);
        self.run_pearls().await
    }

    pub async fn remount(&self) -> Result<()> {
        self.holders.write().await.clear();
        self.run().await
    }

    async fn run_pearls(&self) -> Result<()> {
        let holders = self.holders.write().await;

        for holder in holders.iter() {
            holder.prepare_storage().await?;
            debug!("backend pearl group run pearls storage prepared");
        }
        Ok(())
    }

    pub async fn add(&self, holder: Holder) -> usize {
        let mut holders = self.holders.write().await;
        holders.push(holder);
        holders.len() - 1
    }

    pub async fn add_range(&self, new: Vec<Holder>) {
        let mut holders = self.holders.write().await;
        holders.extend(new);
    }

    // find in all pearls actual pearl and try create new
    async fn get_actual_holder(&self, data: &BobData) -> Result<Holder> {
        self.find_actual_holder(data)
            .or_else(|e| {
                debug!("cannot find pearl: {}", e);
                self.create_write_pearl(data.meta().timestamp())
            })
            .await
    }

    // find in all pearls actual pearl
    async fn find_actual_holder(&self, data: &BobData) -> BackendResult<Holder> {
        let holders = self.holders.read().await;
        holders
            .iter()
            .find(|holder| holder.gets_into_interval(data.meta().timestamp()))
            .cloned()
            .ok_or_else(|| {
                Error::failed(format!(
                    "cannot find actual pearl folder. meta: {}",
                    data.meta().timestamp()
                ))
            })
    }

    // create pearl for current write
    async fn create_write_pearl(&self, ts: u64) -> Result<Holder> {
        let mut indexes = self.created_holder_indexes.write().await;
        let created_holder_index = indexes.get(&ts).copied();
        let index = if let Some(exisiting_index) = created_holder_index {
            exisiting_index
        } else {
            let new_index = self
                .settings
                .config()
                .try_multiple_times_async(
                    || self.try_create_write_pearl(ts),
                    "pearl init failed",
                    self.settings.config().settings().create_pearl_wait_delay(),
                )
                .await?;
            debug!("group create write pearl holder index {}", new_index);
            indexes.insert(ts, new_index);
            debug!("group create write pearl holder inserted");
            new_index
        };
        Ok(self.holders.read().await[index].clone())
    }

    async fn try_create_write_pearl(&self, timestamp: u64) -> Result<usize> {
        info!("creating pearl for timestamp {}", timestamp);
        let pearl = self.create_pearl_by_timestamp(timestamp);
        self.save_pearl(pearl.clone()).await
    }

    async fn save_pearl(&self, holder: Holder) -> Result<usize> {
        holder.prepare_storage().await?;
        debug!("backend pearl group save pearl storage prepared");
        Ok(self.add(holder).await)
    }

    pub async fn put(&self, key: BobKey, data: BobData) -> Result<(), Error> {
        let holder = self
            .get_actual_holder(&data)
            .await
            .map_err(|e| Error::failed(format!("{:#?}", e)))?;
        Self::put_common(holder, key, data).await
    }

    async fn put_common(holder: Holder, key: BobKey, data: BobData) -> Result<(), Error> {
        let result = holder.write(key, data).await;
        if let Err(e) = result {
            if !e.is_duplicate() && !e.is_not_ready() {
                error!("pearl holder will restart: {:?}", e);
                holder.try_reinit().await?;
                holder
                    .prepare_storage()
                    .await
                    .map_err(|e| Error::storage(format!("{:#?}", e)))?;
                debug!("backend pearl group put common storage prepared");
            }
        }
        Ok(())
    }

    pub async fn get(&self, key: BobKey) -> Result<BobData, Error> {
        let holders = self.holders.read().await;
        let mut has_error = false;
        let mut results = vec![];
        for holder in holders.iter() {
            let get = Self::get_common(holder.clone(), key).await;
            match get {
                Ok(data) => {
                    trace!("get data: {:?} from: {:?}", data, holder);
                    results.push(data);
                }
                Err(err) => {
                    if err.is_key_not_found() {
                        debug!("{} not found in {:?}", key, holder)
                    } else {
                        has_error = true;
                        error!("get error: {}, from : {:?}", err, holder);
                    }
                }
            }
        }
        if results.is_empty() {
            if has_error {
                debug!("cannot read from some pearls");
                Err(Error::failed("cannot read from some pearls"))
            } else {
                debug!("not found in any pearl");
                Err(Error::key_not_found(key))
            }
        } else {
            debug!("get with max timestamp, from {} results", results.len());
            Ok(Settings::choose_most_recent_data(results)
                .expect("results cannot be empty, because of the previous check"))
        }
    }

    async fn get_common(holder: Holder, key: BobKey) -> Result<BobData, Error> {
        let result = holder.read(key).await;
        if let Err(e) = &result {
            if !e.is_key_not_found() && !e.is_not_ready() {
                holder.try_reinit().await?;
                holder
                    .prepare_storage()
                    .await
                    .map_err(|e| Error::storage(format!("{:#?}", e)))?;
                debug!("backend pearl group get common storage prepared");
            }
        }
        result
    }

    pub async fn exist(&self, keys: &[BobKey]) -> Vec<bool> {
        let mut exist = vec![false; keys.len()];
        let holders = self.holders.read().await;
        for (ind, &key) in keys.iter().enumerate() {
            for holder in holders.iter() {
                if !exist[ind] {
                    exist[ind] = holder.exist(key).await.unwrap_or(false);
                }
            }
        }
        exist
    }

    pub fn holders(&self) -> Arc<RwLock<Vec<Holder>>> {
        self.holders.clone()
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn disk_name(&self) -> &str {
        &self.disk_name
    }

    pub fn vdisk_id(&self) -> u32 {
        self.vdisk_id
    }

    pub async fn attach(&self, start_timestamp: u64) -> BackendResult<()> {
        let holders = self.holders.read().await;
        if holders
            .iter()
            .any(|holder| holder.start_timestamp() == start_timestamp)
        {
            let msg = format!("pearl:{} already exists", start_timestamp);
            warn!("{}", msg);
            Err(Error::pearl_change_state(msg))
        } else {
            let holder = self.create_pearl_by_timestamp(start_timestamp);
            self.save_pearl(holder)
                .await
                .map_err(|e| Error::storage(format!("{:#?}", e)))?;
            Ok(())
        }
    }

    pub async fn detach(&self, start_timestamp: u64) -> BackendResult<Vec<Holder>> {
        let mut holders = self.holders.write().await;
        debug!("write lock acquired");
        let holders = holders
            .drain_filter(|holder| {
                debug!("{}", holder.start_timestamp());
                holder.start_timestamp() == start_timestamp
                    && !holder.is_actual(self.settings.get_actual_timestamp_start())
            })
            .collect::<Vec<_>>();
        if holders.is_empty() {
            let msg = format!("pearl:{} not found", start_timestamp);
            return Err(Error::pearl_change_state(msg));
        }
        for holder in &holders {
            let lock_guard = holder.storage();
            let pearl_sync = lock_guard.write().await;
            let storage = pearl_sync.storage().clone();
            if let Err(e) = storage.close().await {
                warn!("pearl closed: {:?}", e);
            }
        }
        Ok(holders)
    }

    pub fn create_pearl_holder(&self, start_timestamp: u64, hash: &str) -> Holder {
        let end_timestamp = start_timestamp + self.settings.timestamp_period_as_secs();
        let mut path = self.directory_path.clone();
        info!("creating pearl holder {}", path.as_path().display());
        let partition_name = PartitionName::new(start_timestamp, &hash);
        path.push(partition_name.to_string());
        let mut config = self.settings.config().clone();
        let prefix = config.blob_file_name_prefix().to_owned();
        config.set_blob_file_name_prefix(format!("{}_{}", prefix, hash));
        Holder::new(
            start_timestamp,
            end_timestamp,
            self.vdisk_id,
            path,
            config,
            self.dump_sem.clone(),
        )
    }

    pub(crate) fn create_pearl_by_timestamp(&self, time: u64) -> Holder {
        let start_timestamp =
            Stuff::get_start_timestamp_by_timestamp(self.settings.timestamp_period(), time);
        info!(
            "pearl for timestamp {} will be created with timestamp {}",
            time, start_timestamp
        );
        let hash = self.get_owner_node_hash();
        self.create_pearl_holder(start_timestamp, &hash)
    }

    pub(crate) fn create_current_pearl(&self) -> Holder {
        let start_timestamp = self.settings.get_actual_timestamp_start();
        let hash = self.get_owner_node_hash();
        self.create_pearl_holder(start_timestamp, &hash)
    }

    pub(crate) fn read_vdisk_directory(&self) -> BackendResult<Vec<Holder>> {
        Stuff::check_or_create_directory(&self.directory_path)?;

        let mut holders = vec![];
        let pearl_directories = Settings::get_all_subdirectories(&self.directory_path)?;
        for entry in pearl_directories {
            if let Ok(file_name) = entry
                .file_name()
                .into_string()
                .map_err(|e| warn!("cannot parse file name: {:?}, {:?}", entry, e))
            {
                let partition_name = PartitionName::try_from_string(&file_name);
                if let Some(partition_name) = partition_name {
                    let pearl_holder =
                        self.create_pearl_holder(partition_name.timestamp, &partition_name.hash);
                    holders.push(pearl_holder);
                } else {
                    warn!("failed to parse partition name from {}", file_name);
                }
            }
        }
        Ok(holders)
    }

    fn get_owner_node_hash(&self) -> String {
        let hash = digest(&SHA256, self.owner_node_name.as_bytes());
        let hash = hash.as_ref();
        let mut hex = vec![];
        // Translate bytes to simple digit-letter representation
        for i in (0..hash.len()).step_by(3) {
            let max = std::cmp::min(i + 3, hash.len());
            let bytes = &hash[i..max];
            if !bytes.is_empty() {
                hex.push(ASCII_TRANSLATION[(bytes[0] >> 2) as usize]); // First 6 bits of first byte
                hex.push(
                    ASCII_TRANSLATION[((bytes[0] << 4) & 0b11_0000
                        | (bytes.get(1).unwrap_or(&0) >> 4))
                        as usize],
                ); // Last 2 bits of first byte and first 4 bits of second byte
                if bytes.len() > 1 {
                    hex.push(
                        ASCII_TRANSLATION[(bytes[1] & 0b0000_1111
                            | (bytes.get(2).unwrap_or(&0) >> 2 & 0b11_0000))
                            as usize],
                    ); // Last 4 bits of second byte and first 2 bits of third byte
                    if bytes.len() > 2 {
                        hex.push(ASCII_TRANSLATION[(bytes[2] & 0b0011_1111) as usize]);
                    } // Last 6 bits of third byte
                }
            }
        }
        hex.truncate(self.settings.config().hash_chars_count() as usize);
        String::from_utf8(hex).unwrap()
    }

    pub(crate) async fn close_unneeded_active_blobs(&self, soft: usize, hard: usize) {
        let holders_lock = self.holders();
        let mut holders_write = holders_lock.write().await;
        let holders: &mut Vec<_> = holders_write.as_mut();

        let mut total_open_blobs = 0;
        let mut close = vec![];
        for h in holders.iter_mut() {
            if !h.active_blob_is_empty().await {
                total_open_blobs += 1;
                if h.is_outdated() && h.no_writes_recently().await {
                    close.push(h);
                }
            }
        }
        let soft = soft.saturating_sub(total_open_blobs - close.len());
        let hard = hard.saturating_sub(total_open_blobs - close.len());

        debug!(
            "closing outdated blobs according to limits ({}, {})",
            soft, hard
        );

        let mut is_small = vec![];
        for h in &close {
            is_small.push(h.active_blob_is_small().await);
        }

        let mut close: Vec<_> = close.into_iter().enumerate().collect();
        Self::sort_by_priority(&mut close, &is_small);

        while close.len() > hard {
            let (_, holder) = close.pop().unwrap();
            holder.close_active_blob().await;
            info!("active blob of {} closed by hard cap", holder.get_id());
        }

        while close.len() > soft && close.last().map_or(false, |(ind, _)| !is_small[*ind]) {
            let (_, holder) = close.pop().unwrap();
            holder.close_active_blob().await;
            info!("active blob of {} closed by soft cap", holder.get_id());
        }
    }

    fn sort_by_priority(close: &mut [(usize, &mut Holder)], is_small: &[bool]) {
        use std::cmp::Ordering;
        close.sort_by(|(i, x), (j, y)| match (is_small[*i], is_small[*j]) {
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            _ => x.end_timestamp().cmp(&y.end_timestamp()),
        });
    }
}

lazy_static! {
    static ref ASCII_TRANSLATION: Vec<u8> = (0..=255)
        .filter(|&i| (i > 47 && i < 58) // numbers
            || (i > 64 && i < 91) // upper case letters
            || (i > 96 && i < 123) // lower case letters
            || (i == 45) // -
            || (i == 43)) // +
        .collect();
}

impl Display for Group {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        f.debug_struct("Group")
            .field("vdisk_id", &self.vdisk_id)
            .field("node_name", &self.node_name)
            .field("directory_path", &self.directory_path)
            .field("disk_name", &self.disk_name)
            .field("..", &"some fields ommited")
            .finish()
    }
}

struct PartitionName {
    timestamp: u64,
    hash: String,
}

impl PartitionName {
    fn new(timestamp: u64, hash: &str) -> Self {
        Self {
            timestamp,
            hash: hash.to_string(),
        }
    }

    fn try_from_string(s: &str) -> Option<Self> {
        let mut iter = s.split('_');
        let timestamp_string = iter.next();
        timestamp_string.and_then(|timestamp_string| {
            let hash_string = iter.next().unwrap_or("");
            timestamp_string
                .parse()
                .map_err(|e| warn!("failed to parse timestamp, {:?}", e))
                .ok()
                .map(|timestamp| Self {
                    timestamp,
                    hash: hash_string.to_string(),
                })
        })
    }
}

impl Display for PartitionName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}_{}", self.timestamp, self.hash)
    }
}
