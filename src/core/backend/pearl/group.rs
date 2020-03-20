use super::prelude::*;

#[derive(Clone, Debug)]
pub(crate) struct Group {
    holders: Arc<RwLock<Vec<Holder>>>,
    pearl_sync: Arc<SyncState>,
    settings: Arc<Settings>,
    directory_path: PathBuf,
    vdisk_id: VDiskId,
    node_name: String,
    disk_name: String,
}

impl Group {
    pub fn new(
        settings: Arc<Settings>,
        vdisk_id: VDiskId,
        node_name: String,
        disk_name: String,
        directory_path: PathBuf,
    ) -> Self {
        Self {
            holders: Arc::new(RwLock::new(vec![])),
            pearl_sync: Arc::new(SyncState::new()),
            settings,
            vdisk_id,
            node_name,
            directory_path,
            disk_name,
        }
    }

    pub fn can_process_operation(&self, operation: &BackendOperation) -> bool {
        if operation.is_data_alien() {
            if let Some(node_name) = operation.remote_node_name() {
                *node_name == self.node_name
            } else {
                self.vdisk_id == operation.vdisk_id()
            }
        } else {
            self.disk_name == operation.disk_name_local() && self.vdisk_id == operation.vdisk_id()
        }
    }

    // @TODO limit number of holder creation retry attempts
    pub async fn run(&self) {
        let duration = self.settings.config().fail_retry_timeout();

        let mut holders = Vec::new();

        debug!("{}: read holders from disk", self);
        while let Err(e) = self.read_vdisk_directory().map(|read_holders| {
            holders = read_holders;
        }) {
            error!(
                "{}: can't create pearl holders: {:?}, await for {}ms",
                self,
                e,
                duration.as_millis()
            );
            delay_for(duration).await;
        }
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
        self.run_pearls().await;
    }

    async fn run_pearls(&self) {
        let holders = self.holders.write().await;

        for holder in holders.iter() {
            holder.prepare_storage().await;
        }
    }

    pub async fn add(&self, holder: Holder) {
        let mut holders = self.holders.write().await;
        holders.push(holder);
    }

    pub async fn add_range(&self, new: Vec<Holder>) {
        let mut holders = self.holders.write().await;
        holders.extend(new);
    }

    // find in all pearls actual pearl and try create new
    async fn get_actual_holder(&self, data: &BobData) -> BackendResult<Holder> {
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
                Error::Failed(format!(
                    "cannot find actual pearl folder. meta: {}",
                    data.meta().timestamp()
                ))
            })
    }

    // @TODO limit try init attempts
    // create pearl for current write
    async fn create_write_pearl(&self, timestamp: u64) -> BackendResult<Holder> {
        loop {
            if self.pearl_sync.try_init().await {
                let pearl = self.create_pearl_by_timestamp(timestamp);
                self.save_pearl(pearl.clone()).await;
                self.pearl_sync.mark_as_created().await;
                return Ok(pearl);
            } else {
                let t = self.settings.config().settings().create_pearl_wait_delay();
                warn!("pearl init failed, retry in {}ms", t.as_millis());
                delay_for(t).await;
            }
        }
    }

    async fn save_pearl(&self, holder: Holder) {
        holder.prepare_storage().await;
        self.add(holder).await;
    }

    pub async fn put(&self, key: BobKey, data: BobData) -> PutResult {
        let holder = self.get_actual_holder(&data).await?;

        Self::put_common(holder, key, data).await
    }

    async fn put_common(holder: Holder, key: BobKey, data: BobData) -> PutResult {
        let result = holder.write(key, data).await;
        if let Err(e) = result {
            if e.is_put_error_need_restart() {
                holder.try_reinit().await?;
                holder.prepare_storage().await;
            }
        }
        Ok(())
    }

    pub async fn get(&self, key: BobKey) -> GetResult {
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
                Err(BackendError::KeyNotFound(key)) => debug!("{} not found in {:?}", key, holder),
                Err(err) => {
                    has_error = true;
                    error!("get error: {}, from : {:?}", err, holder);
                }
            }
        }
        if results.is_empty() {
            if has_error {
                debug!("cannot read from some pearls");
                Err(Error::Failed("cannot read from some pearls".to_string()))
            } else {
                debug!("not found in any pearl");
                Err(Error::KeyNotFound(key))
            }
        } else {
            debug!("get with max timestamp, from {} results", results.len());
            Ok(Settings::choose_most_recent_data(results)
                .expect("results cannot be empty, because of the previous check"))
        }
    }

    async fn get_common(holder: Holder, key: BobKey) -> GetResult {
        let result = holder.read(key).await;
        if let Err(e) = &result {
            if e.is_get_error_need_restart() {
                holder.try_reinit().await?;
                holder.prepare_storage().await;
            }
        }
        result
    }

    pub async fn exist(&self, keys: &[BobKey]) -> Vec<bool> {
        let mut exist = vec![false; keys.len()];
        let holders = self.holders.read().await;
        for (ind, &key) in keys.iter().enumerate() {
            for holder in holders.iter() {
                exist[ind] = holder.exist(key).await.unwrap_or(false);
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
            Err(Error::PearlChangeState(msg))
        } else {
            let holder = self.create_pearl_by_timestamp(start_timestamp);
            self.save_pearl(holder).await;
            Ok(())
        }
    }

    pub async fn detach(&self, start_timestamp: u64) -> BackendResult<Holder> {
        let mut holders = self.holders.write().await;
        debug!("write lock acquired");
        if let Some(holder) = holders.iter().find(|holder| {
            debug!("{}", holder.start_timestamp());
            holder.start_timestamp() == start_timestamp
        }) {
            if holder.is_actual(self.settings.get_actual_timestamp_start()) {
                let msg = format!("active pearl:{} cannot be detached", start_timestamp);
                warn!("{}", msg);
                Err(Error::PearlChangeState(msg))
            } else {
                {
                    let lock_guard = holder.storage();
                    let pearl_sync = lock_guard.write().await;
                    let storage = pearl_sync.storage();
                    if let Err(e) = storage.close().await {
                        warn!("pearl closed: {:?}", e);
                    }
                }
                let mut holders_to_return = holders
                    .drain_filter(|holder| holder.start_timestamp() == start_timestamp)
                    .collect::<Vec<_>>();
                holders_to_return.pop().ok_or_else(|| {
                    Error::PearlChangeState(format!(
                        "error detaching pearl with timestamp {}",
                        start_timestamp
                    ))
                })
            }
        } else {
            let msg = format!("pearl:{} not found", start_timestamp);
            Err(Error::PearlChangeState(msg))
        }
    }

    pub fn create_pearl_holder(&self, start_timestamp: u64) -> Holder {
        let end_timestamp = start_timestamp + self.settings.timestamp_period_as_secs();
        let mut path = self.directory_path.clone();
        path.push(format!("{}/", start_timestamp));

        Holder::new(
            start_timestamp,
            end_timestamp,
            self.vdisk_id,
            path,
            self.settings.config().clone(),
        )
    }

    pub(crate) fn create_pearl_by_timestamp(&self, time: u64) -> Holder {
        let start_timestamp =
            Stuff::get_start_timestamp_by_timestamp(self.settings.timestamp_period(), time);
        self.create_pearl_holder(start_timestamp)
    }

    pub(crate) fn create_current_pearl(&self) -> Holder {
        let start_timestamp = self.settings.get_actual_timestamp_start();
        self.create_pearl_holder(start_timestamp)
    }

    pub(crate) fn read_vdisk_directory(&self) -> BackendResult<Vec<Holder>> {
        Stuff::check_or_create_directory(&self.directory_path)?;

        let mut holders = vec![];
        let pearl_directories = Settings::get_all_subdirectories(&self.directory_path)?;
        for entry in pearl_directories {
            if let Ok(file_name) = entry
                .file_name()
                .into_string()
                .map_err(|_| warn!("cannot parse file name: {:?}", entry))
            {
                let start_timestamp = file_name
                    .parse()
                    .map_err(|_| warn!("cannot parse file name: {:?} as timestamp", entry))
                    .expect("parse file name");
                let pearl_holder = self.create_pearl_holder(start_timestamp);
                holders.push(pearl_holder);
            }
        }
        Ok(holders)
    }
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
