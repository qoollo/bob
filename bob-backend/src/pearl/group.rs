use crate::{pearl::utils::get_current_timestamp, prelude::*};

use super::{data::Key, utils::StartTimestampConfig, Holder, Hooks};
use crate::{
    core::Operation,
    pearl::{core::BackendResult, settings::Settings, utils::Utils},
};
use futures::Future;
use pearl::{BloomProvider, ReadResult};
use ring::digest::{digest, SHA256};
use async_std::sync::{RwLock as UgradableRwLock, RwLockUpgradableReadGuard};

pub type HoldersContainer =
    HierarchicalFilters<Key, <Holder as BloomProvider<Key>>::Filter, Holder>;

#[derive(Clone, Debug)]
pub struct Group {
    holders: Arc<UgradableRwLock<HoldersContainer>>,
    reinit_lock: Arc<RwLock<()>>,
    settings: Arc<Settings>,
    directory_path: PathBuf,
    vdisk_id: VDiskId,
    node_name: String,
    disk_name: String,
    owner_node_name: String,
    dump_sem: Arc<Semaphore>,
}

impl Group {
    pub fn new(
        settings: Arc<Settings>,
        vdisk_id: VDiskId,
        node_name: String,
        disk_name: String,
        directory_path: PathBuf,
        owner_node_name: String,
        dump_sem: Arc<Semaphore>,
    ) -> Self {
        Self {
            holders: Arc::new(UgradableRwLock::new(HoldersContainer::new(
                settings.holder_group_size(),
                2,
            ))),
            reinit_lock: Arc::new(RwLock::new(())),
            settings,
            vdisk_id,
            node_name,
            directory_path,
            disk_name,
            owner_node_name,
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
            self.vdisk_id == operation.vdisk_id() && self.disk_name == operation.disk_name_local()
        }
    }

    async fn run_under_reinit_lock(&self, pp: impl Hooks) -> AnyResult<()> {
        debug!("{}: read holders from disk", self);
        let config = self.settings.config();
        let new_holders = config
            .try_multiple_times_async(
                || self.read_vdisk_directory(),
                "can't create pearl holders",
                self.settings.config().fail_retry_timeout(),
            )
            .await
            .with_context(|| "backend pearl group read vdisk directory failed")?;
        debug!("{}: count holders: {}", self, new_holders.len());
        debug!("{}: save holders to group", self);
        let mut holders = self.holders.write().await;
        holders.clear();
        holders.extend(new_holders).await;   
        debug!("{}: start holders", self);
        Self::run_pearls(&mut holders, pp).await
    }

    pub async fn run(&self, pp: impl Hooks) -> AnyResult<()> {
        let _reinit_lock = self.reinit_lock.write().await;
        self.run_under_reinit_lock(pp).await
    }

    pub async fn remount(&self, pp: impl Hooks) -> AnyResult<()> {
        let _reinit_lock = self.reinit_lock.write().await;
        let cleared = self.holders.write().await.clear_and_get_values();
        close_holders(cleared.into_iter()).await;
        self.run_under_reinit_lock(pp).await
    }

    async fn run_pearls(holders: &mut HoldersContainer, pp: impl Hooks) -> AnyResult<()> {
        for holder in holders.iter() {
            holder.prepare_storage().await?;
            pp.storage_prepared(holder).await;
            debug!("backend pearl group run pearls storage prepared");
        }
        holders.reload().await;
        Ok(())
    }

    pub(crate) async fn for_each_holder<F, Fut>(&self, f: F)
    where
        F: Fn(&Holder) -> Fut + Clone,
        Fut: Future<Output = ()>,
    {
        self.holders()
            .read()
            .await
            .iter()
            .map(f)
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<()>>()
            .await;
    }

    // find in all pearls actual pearl and try create new
    async fn get_or_create_actual_holder(
        &self,
        data_timestamp: u64,
        timestamp_config: StartTimestampConfig,
    ) -> Result<(ChildId, Holder), Error> {
        let holder_info = {
            let holders = self.holders.read().await;
            Self::find_actual_holder(&holders, data_timestamp).await
        };
        if let Err(e) = holder_info {
            debug!("cannot find pearl: {}", e);
            self.get_or_create_write_pearl(data_timestamp, timestamp_config).await
        } else {
            holder_info
        }
    }

    // find in all pearls actual pearl
    async fn find_actual_holder(holders: &HoldersContainer, data_timestamp: u64) -> BackendResult<(ChildId, Holder)> {
        let mut holders_for_time = holders
            .iter()
            .enumerate()
            .filter(|h| h.1.gets_into_interval(data_timestamp));

        if let Some(mut max) = holders_for_time.next() {
            for elem in holders_for_time {
                if elem.1.start_timestamp() > max.1.start_timestamp() {
                    max = elem
                }
            }
            Ok((max.0, max.1.clone()))
        } else {
            Err(Error::failed(format!(
                "cannot find actual pearl folder. meta: {}",
                data_timestamp
            )))
        }
    }

    async fn create_and_init_pearl(
        &self,
        data_timestamp: u64,
        timestamp_config: &StartTimestampConfig
    ) -> Result<Holder, Error> {
        let pearl = self.create_pearl_by_timestamp(data_timestamp, &timestamp_config);
        pearl.prepare_storage().await?;
        Ok(pearl)
    }

    // create or get existing pearl for current write
    async fn get_or_create_write_pearl(
        &self,
        data_timestamp: u64,
        timestamp_config: StartTimestampConfig,
    ) -> Result<(ChildId, Holder), Error> {
        // importantly, only one thread can hold an upgradable lock at a time
        let holders = self.holders.upgradable_read().await;
        
        let created_holder_index = Self::find_actual_holder(&holders, data_timestamp).await;
        Ok(if let Ok(index_and_holder) = created_holder_index {
            index_and_holder
        } else {
            info!("creating pearl for timestamp {}", data_timestamp);
            let pearl = self.settings.config()
                .try_multiple_times_async(
                    || self.create_and_init_pearl(data_timestamp, &timestamp_config),
                    "pearl init failed",
                    self.settings.config().settings().create_pearl_wait_delay(),
                ).await?;
            debug!("backend pearl group save pearl storage prepared");    
            let mut holders = RwLockUpgradableReadGuard::upgrade(holders).await;
            let new_index = holders.push(pearl.clone()).await;
            debug!("group create write pearl holder inserted, index {:?}", new_index);
            (new_index, pearl)
        })
    }

    pub async fn put(
        &self,
        key: BobKey,
        data: &BobData,
        timestamp_config: StartTimestampConfig,
    ) -> Result<(), Error> {
        let _reinit_lock = self.reinit_lock.try_read().map_err(|_| Error::holder_temporary_unavailable())?;
        let holder = self
            .get_or_create_actual_holder(data.meta().timestamp(), timestamp_config)
            .await?;
        let res = Self::put_common(&holder.1, key, data).await?;
        self.holders
            .read()
            .await
            .add_to_parents(holder.0, &Key::from(key));
        Ok(res)
    }

    async fn put_common(holder: &Holder, key: BobKey, data: &BobData) -> Result<(), Error> {
        let result = holder.write(key, data).await;
        if let Err(e) = result {
            // if we receive WorkDirUnavailable it's likely disk error, so we shouldn't restart one
            // holder but instead try to restart the whole disk
            if !e.is_possible_disk_disconnection() && !e.is_duplicate() && !e.is_not_ready() {
                error!("pearl holder will restart: {:?}", e);
                holder.try_reinit().await?;
                debug!("backend pearl group put common storage prepared");
            }
            Err(e)
        } else {
            Ok(())
        }
    }

    pub async fn get(&self, key: BobKey) -> Result<BobData, Error> {
        let _reinit_lock = self.reinit_lock.try_read().map_err(|_| Error::holder_temporary_unavailable())?;
        let holders = self.holders.read().await;
        let mut has_error = false;
        let mut max_timestamp = None;
        let mut result = None;
        for holder in holders
            .iter_possible_childs_rev(&Key::from(key))
            .map(|(_, x)| &x.data)
        {
            let get = Self::get_common(&holder, key).await;
            match get {
                Ok(data) => match data {
                    ReadResult::Found(data) => {
                        trace!("get data: {:?} from: {:?}", data, holder);
                        let ts = data.meta().timestamp();
                        if max_timestamp.is_none() || ts > max_timestamp.unwrap() {
                            max_timestamp = Some(ts);
                            result = Some(data);
                        }
                    }
                    ReadResult::Deleted(ts) => {
                        trace!("{} is deleted in {:?} at {}", key, holder, ts);
                        let ts: u64 = ts.into();
                        if max_timestamp.is_none() || ts > max_timestamp.unwrap() {
                            max_timestamp = Some(ts);
                            result = None;
                        }
                    }
                    ReadResult::NotFound => {
                        debug!("{} not found in {:?}", key, holder)
                    }
                },
                Err(err) => {
                    has_error = true;
                    error!("get error: {}, from : {:?}", err, holder);
                }
            }
        }
        if let Some(result) = result {
            Ok(result)
        } else {
            if has_error {
                debug!("cannot read from some pearls");
                Err(Error::failed("cannot read from some pearls"))
            } else {
                Err(Error::key_not_found(key))
            }
        }
    }

    async fn get_common(holder: &Holder, key: BobKey) -> Result<ReadResult<BobData>, Error> {
        let result = holder.read(key).await;
        if let Err(e) = &result {
            if !e.is_key_not_found() && !e.is_not_ready() {
                holder.try_reinit().await?;
                debug!("backend pearl group get common storage prepared");
            }
        }
        result
    }

    pub async fn exist(&self, keys: &[BobKey]) -> Result<Vec<bool>, Error> {
        let _reinit_lock = self.reinit_lock.try_read().map_err(|_| Error::holder_temporary_unavailable())?;
        let mut exist = vec![false; keys.len()];
        let holders = self.holders.read().await;
        for (ind, &key) in keys.iter().enumerate() {
            let mut max_timestamp = None;
            let mut result = false;
            for (_, Leaf { data: holder, .. }) in holders.iter_possible_childs_rev(&Key::from(key))
            {
                match holder.exist(key).await.unwrap_or(ReadResult::NotFound) {
                    ReadResult::Found(ts) => {
                        let ts: u64 = ts.into();
                        if max_timestamp.is_none() || ts > max_timestamp.unwrap() {
                            max_timestamp = Some(ts);
                            result = true;
                        }
                    }
                    ReadResult::Deleted(ts) => {
                        let ts: u64 = ts.into();
                        if max_timestamp.is_none() || ts > max_timestamp.unwrap() {
                            max_timestamp = Some(ts);
                            result = false;
                        }
                    }
                    ReadResult::NotFound => continue,
                }
            }
            exist[ind] = result;
        }
        Ok(exist)
    }


    pub async fn delete(
        &self,
        key: BobKey,
        meta: &BobMeta,
        timestamp_config: StartTimestampConfig,
        force_delete: bool,
    ) -> Result<u64, Error> {
        let _reinit_lock = self.reinit_lock.try_read().map_err(|_| Error::holder_temporary_unavailable())?;
        let mut reference_timestamp = meta.timestamp();
        let mut total_deletion_count = 0;

        if force_delete {
            let actual_holder = self.get_or_create_actual_holder(meta.timestamp(), timestamp_config).await?;
            reference_timestamp = actual_holder.1.start_timestamp();
            total_deletion_count += self.delete_in_actual_holder(actual_holder, key, meta).await?;
        }

        let all_holders_before: Vec<_> = {
            let holders = self.holders.read().await;
            holders.iter().cloned().enumerate().filter(|h| h.1.start_timestamp() < reference_timestamp).collect()
        };

        total_deletion_count += self.delete_in_holders_before(all_holders_before, key, meta).await.unwrap_or(0);

        return Ok(total_deletion_count);
    }

    async fn delete_in_actual_holder(&self, holder: (ChildId, Holder), key: BobKey, meta: &BobMeta) -> Result<u64, Error> {
        // In actual holder we delete with force_delete = true
        let delete_count = Self::delete_common(holder.1.clone(), key, meta, true).await?;
            // We need to add marker record to alien regardless of record presence
        self.holders
            .read()
            .await
            .add_to_parents(holder.0, &Key::from(key));
        
        Ok(delete_count)
    }

    async fn delete_in_holders_before(
        &self,
        holders: Vec<(ChildId, Holder)>,
        key: BobKey,
        meta: &BobMeta
    ) -> Result<u64, Error> {
        let mut total_count = 0;
        // General assumption is that processing in order from new to old holders is better, but this is not strictly required
        for holder in holders.into_iter().rev() {
            let holder_id = holder.1.get_id();
            let delete = Self::delete_common(holder.1, key, meta, false).await;
            total_count += match delete {
                Ok(count) => {
                    trace!("delete data: {} from: {}", count, holder_id);
                    count
                }
                Err(err) => {
                    debug!("delete error: {}, from : {}", err, holder_id);
                    0
                }
            };
        }
        Ok(total_count)
    }

    async fn delete_common(holder: Holder, key: BobKey, meta: &BobMeta, force_delete: bool) -> Result<u64, Error> {
        let result = holder.delete(key, meta, force_delete).await;
        if let Err(e) = result {
            // if we receive WorkDirUnavailable it's likely disk error, so we shouldn't restart one
            // holder but instead try to restart the whole disk
            if !e.is_possible_disk_disconnection() && !e.is_duplicate() && !e.is_not_ready() {
                error!("pearl holder will restart: {:?}", e);
                holder.try_reinit().await?;
                debug!("backend::pearl::group::delete_common storage prepared");
            }
            Err(e)
        } else {
            result
        }
    }
    

    pub fn holders(&self) -> Arc<UgradableRwLock<HoldersContainer>> {
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
        // importantly, only one thread can hold an upgradable lock at a time
        let holders = self.holders.upgradable_read().await;
        if holders
            .iter()
            .map(|x| x.start_timestamp())
            .any(|timestamp| timestamp == start_timestamp)
        {
            let msg = format!("pearl:{} already exists", start_timestamp);
            warn!("{}", msg);
            Err(Error::pearl_change_state(msg))
        } else {
            let holder =
                self.create_pearl_by_timestamp(start_timestamp, &StartTimestampConfig::default());
            holder.prepare_storage().await?;
            debug!("backend pearl group save pearl storage prepared");
            let mut holders = RwLockUpgradableReadGuard::upgrade(holders).await;
            holders.push(holder).await;
            Ok(())
        }
    }

    pub async fn detach(&self, start_timestamp: u64) -> BackendResult<()> {
        let mut holders = self.holders.write().await;
        debug!("write lock acquired");
        let ts = get_current_timestamp();
        let mut removed = vec![];
        for ind in 0..holders.len() {
            if let Some(holder) = holders.get_child(ind) {
                if holder.data.start_timestamp() == start_timestamp
                    && !holder.data.gets_into_interval(ts)
                {
                    removed.push(holders.remove(ind).expect("should be presented"));
                }
            }
        }
        if removed.is_empty() {
            let msg = format!("pearl:{} not found", start_timestamp);
            return Err(Error::pearl_change_state(msg));
        }
        close_holders(removed.into_iter()).await;
        Ok(())
    }

    pub async fn detach_all(&self) -> BackendResult<()> {
        let mut holders_lock = self.holders.write().await;
        let holders: Vec<_> = holders_lock.clear_and_get_values();
        close_holders(holders.into_iter()).await;
        Ok(())
    }

    pub fn create_pearl_holder(&self, start_timestamp: u64, hash: &str) -> Holder {
        let end_timestamp = start_timestamp + self.settings.timestamp_period_as_secs();
        let mut path = self.directory_path.clone();
        info!("creating pearl holder {}", path.as_path().display());
        let partition_name = PartitionName::new(start_timestamp, hash);
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

    pub fn create_pearl_by_timestamp(
        &self,
        time: u64,
        timestamp_config: &StartTimestampConfig,
    ) -> Holder {
        let start_timestamp = Utils::get_start_timestamp_by_timestamp(
            self.settings.timestamp_period(),
            time,
            timestamp_config,
        );
        info!(
            "pearl for timestamp {} will be created with timestamp {}",
            time, start_timestamp
        );
        let hash = self.get_owner_node_hash();
        self.create_pearl_holder(start_timestamp, &hash)
    }

    pub fn create_current_pearl(&self) -> Holder {
        let start_timestamp = self.settings.get_actual_timestamp_start();
        let hash = self.get_owner_node_hash();
        self.create_pearl_holder(start_timestamp, &hash)
    }

    pub async fn read_vdisk_directory(&self) -> BackendResult<Vec<Holder>> {
        Utils::check_or_create_directory(&self.directory_path).await?;

        let mut holders = vec![];
        let pearl_directories = Settings::get_all_subdirectories(&self.directory_path).await?;
        for entry in pearl_directories {
            if let Ok(file_name) = entry
                .file_name()
                .into_string()
                .map_err(|e| warn!("cannot parse file name: {:?}, {:?}", entry, e))
            {
                if let Some(name) = PartitionName::try_from_string(&file_name) {
                    let pearl_holder = self.create_pearl_holder(name.timestamp, &name.hash);
                    holders.push(pearl_holder);
                } else {
                    warn!("failed to parse partition name from {}", file_name);
                }
            }
        }
        holders.sort_by(|a, b| a.start_timestamp().cmp(&b.start_timestamp()));
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

    pub(crate) async fn find_oldest_inactive_holder(&self) -> Option<Holder> {
        let holders_lock = self.holders();
        let holders = holders_lock.read().await;
        let mut result: Option<Holder> = None;
        let period = self.settings.timestamp_period_as_secs() / 2 + 10;
        for holder in holders.iter() {
            if holder.has_active_blob().await {
                if holder.is_outdated() && holder.is_older_than(period) {
                    if holder.end_timestamp()
                        < result
                            .as_ref()
                            .map(|h| h.end_timestamp())
                            .unwrap_or(u64::MAX)
                    {
                        result = Some(holder.clone());
                    }
                }
            }
        }
        result
    }

    pub(crate) async fn corrupted_blobs_count(&self) -> usize {
        let mut corrupted_blobs = 0;
        let holders = self.holders.read().await;
        for holder in holders.iter() {
            corrupted_blobs += holder.corrupted_blobs_count().await;
        }
        corrupted_blobs
    }

    pub(crate) async fn blobs_count(&self) -> usize {
        let mut blobs = 0;
        let holders = self.holders.read().await;
        for holder in holders.iter() {
            blobs += holder.blobs_count().await;
        }
        blobs
    }

    pub(crate) async fn index_memory(&self) -> usize {
        let mut index_memory = 0;
        let holders = self.holders.read().await;
        for holder in holders.iter() {
            index_memory += holder.index_memory().await;
        }
        index_memory
    }

    pub(crate) async fn find_least_modified_freeable_holder(&self) -> Option<Holder> {
        let holders_lock = self.holders();
        let holders = holders_lock.read().await;
        let mut result: Option<Holder> = None;
        let mut min_modification = u64::MAX;
        let period = self.settings.timestamp_period_as_secs() / 2 + 10;
        for holder in holders.iter() {
            if holder.is_outdated() && holder.is_older_than(period) {
                let last_modification = holder.last_modification();
                if holder.has_excess_resources().await && last_modification < min_modification {
                    min_modification = last_modification;
                    result = Some(holder.clone());
                }
            }
        }
        result
    }

    pub(crate) async fn close_unneeded_active_blobs(&self, soft: usize, hard: usize) {
        let holders_lock = self.holders();
        let holders = holders_lock.read().await;

        let mut total_open_blobs = 0;
        let mut close = vec![];
        for h in holders.iter() {
            if h.has_active_blob().await {
                total_open_blobs += 1;
                if h.is_outdated() && h.no_modifications_recently().await {
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
            is_small.push(h.active_blob_is_small().await.unwrap_or_default());
        }

        let mut close: Vec<_> = close.into_iter().enumerate().collect();
        Self::sort_by_priority(&mut close, &is_small);

        while close.len() > hard {
            let (_, holder) = close.pop().expect("Vector is empty!");
            holder.close_active_blob().await;
            info!("active blob of {} closed by hard cap", holder.get_id());
        }

        while close.len() > soft && close.last().map_or(false, |(ind, _)| !is_small[*ind]) {
            let (_, holder) = close.pop().unwrap();
            holder.close_active_blob().await;
            info!("active blob of {} closed by soft cap", holder.get_id());
        }
    }

    fn sort_by_priority(close: &mut [(usize, &Holder)], is_small: &[bool]) {
        use std::cmp::Ordering;
        close.sort_by(|(i, x), (j, y)| match (is_small[*i], is_small[*j]) {
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            _ => x.end_timestamp().cmp(&y.end_timestamp()),
        });
    }

    pub(crate) async fn filter_memory_allocated(&self) -> usize {
        self.holders.read().await.filter_memory_allocated().await
    }

    pub async fn disk_used(&self) -> u64 {
        self.holders
            .read()
            .await
            .iter()
            .map(|h| h.disk_used())
            .collect::<FuturesUnordered<_>>()
            .fold(0, |acc, x| async move { acc + x })
            .await
    }
}

async fn close_holders(holders: impl Iterator<Item = Holder>) {
    for holder in holders {
        holder.close_storage().await;
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
