use super::prelude::*;
use crate::core_inner::backend::core::ExistResult;

/// Wrap pearl holder and add timestamp info
#[derive(Clone, Debug)]
pub(crate) struct PearlTimestampHolder {
    pub pearl: PearlHolder,
    pub start_timestamp: i64,
    pub end_timestamp: i64,
} //TODO add path and fix Display

impl PearlTimestampHolder {
    pub(crate) fn new(pearl: PearlHolder, start_timestamp: i64, end_timestamp: i64) -> Self {
        Self {
            pearl,
            start_timestamp,
            end_timestamp,
        }
    }
}

impl Display for PearlTimestampHolder {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}", self.start_timestamp)
    }
}

/// Composition of pearls. Add put/get api
#[derive(Clone, Debug)]
pub(crate) struct PearlGroup {
    /// all pearls
    pearls: Arc<RwLock<Vec<PearlTimestampHolder>>>,
    // holds state when we create new pearl
    pearl_sync: Arc<SyncState>,

    settings: Arc<Settings>,
    config: PearlConfig,

    vdisk_id: VDiskId,
    node_name: String,
    pub directory_path: PathBuf,
    disk_name: String,
}

impl PearlGroup {
    pub fn new(
        settings: Arc<Settings>,
        vdisk_id: VDiskId,
        node_name: String,
        disk_name: String,
        directory_path: PathBuf,
        config: PearlConfig,
    ) -> Self {
        Self {
            pearls: Arc::new(RwLock::new(vec![])),
            pearl_sync: Arc::new(SyncState::new()),
            settings,
            vdisk_id,
            node_name,
            directory_path,
            config,
            disk_name,
        }
    }

    pub fn can_process_operation(&self, operation: &BackendOperation) -> bool {
        if operation.is_data_alien() {
            if let Some(ref node_name) = operation.remote_node_name {
                *node_name == self.node_name
            } else {
                self.vdisk_id == operation.vdisk_id
            }
        } else {
            self.disk_name == operation.disk_name_local() && self.vdisk_id == operation.vdisk_id
        }
    }

    pub async fn run(&self) {
        let t = self.config.fail_retry_timeout();

        let mut pearls = Vec::new();

        debug!("{}: read pearls from disk", self);
        while let Err(e) = self.read_vdisk_directory().map(|read_pearls| {
            pearls = read_pearls;
        }) {
            error!("{}: can't create pearls: {:?}", self, e);
            delay_for(t).await;
        }
        debug!("{}: count pearls: {}", self, pearls.len());

        debug!("{}: check current pearl for write", self);
        if pearls
            .iter()
            .all(|pearl| self.settings.is_actual_pearl(pearl))
        {
            self.create_current_pearl();
        }

        debug!("{}: save pearls to group", self);
        self.add_range(pearls.clone()).await;

        debug!("{}: start pearls", self);
        while let Err(err) = self.run_pearls().await {
            error!("{}: can't start pearls: {:?}", self, err);
            delay_for(t).await;
        }
    }

    async fn run_pearls(&self) -> BackendResult<()> {
        let holders = self.pearls.write().await;

        for holder in holders.iter() {
            let pearl = holder.pearl.clone();
            pearl.prepare_storage().await;
        }
        Ok(())
    }

    pub fn create_pearl_by_path(&self, path: PathBuf) -> PearlHolder {
        PearlHolder::new(self.vdisk_id.clone(), path, self.config.clone())
    }

    pub async fn add(&self, pearl: PearlTimestampHolder) {
        let mut pearls = self.pearls.write().await;
        pearls.push(pearl);
    }

    pub async fn add_range(&self, new_pearls: Vec<PearlTimestampHolder>) {
        let mut pearls = self.pearls.write().await;
        pearls.extend(new_pearls);
    }

    /// find in all pearls actual pearl and try create new
    async fn try_get_current_pearl(&self, data: &BobData) -> BackendResult<PearlTimestampHolder> {
        self.find_current_pearl(data)
            .or_else(|e| {
                debug!("cannot find pearl: {}", e);
                self.create_current_write_pearl(data)
                    .and_then(|_| self.find_current_pearl(data))
            })
            .await
    }

    /// find in all pearls actual pearl
    async fn find_current_pearl(&self, data: &BobData) -> BackendResult<PearlTimestampHolder> {
        let pearls = self.pearls.read().await;
        pearls
            .iter()
            .find(|pearl| Settings::is_actual(pearl, &data))
            .cloned()
            .ok_or_else(|| {
                Error::Failed(format!(
                    "cannot find actual pearl folder. meta: {}",
                    data.meta
                ))
            })
    }

    /// create pearl for current write
    async fn create_current_write_pearl(&self, data: &BobData) -> BackendResult<()> {
        // check if pearl is currently creating
        if self.pearl_sync.try_init().await? {
            // check if pearl created
            if self.find_current_pearl(&data).await.is_err() {
                let pearl = self.create_pearl_by_timestamp(data.meta.timestamp);
                self.save_pearl(pearl).await?;
            }
            self.pearl_sync.mark_as_created().await?;
        } else {
            let t = self.config.settings().create_pearl_wait_delay();
            delay_for(t).await;
        }
        Ok(())
    }

    async fn save_pearl(&self, holder: PearlTimestampHolder) -> BackendResult<()> {
        let pearl = holder.pearl.clone();
        self.add(holder).await;
        pearl.prepare_storage().await;
        Ok(())
    }

    pub async fn put(&self, key: BobKey, data: BobData) -> PutResult {
        let holder = self.try_get_current_pearl(&data).await?;

        Self::put_common(holder.pearl, key, data).await
    }

    async fn put_common(holder: PearlHolder, key: BobKey, data: BobData) -> PutResult {
        let result = holder.write(key, data).await.map(|_| BackendPutResult {});
        if Error::is_put_error_need_restart(result.as_ref().err()) && holder.try_reinit().await? {
            holder.reinit_storage()?;
        }
        result
    }

    pub async fn get(&self, key: BobKey) -> GetResult {
        let holders = self.pearls.read().await;
        let mut has_error = false;
        let mut results = vec![];
        for holder in holders.iter() {
            let get = Self::get_common(holder.pearl.clone(), key).await;
            match get {
                Ok(data) => {
                    trace!("get data: {} from: {}", data, holder);
                    results.push(data);
                }
                Err(err) if err != BackendError::KeyNotFound(key) => {
                    has_error = true;
                    debug!("get error: {}, from : {}", err, holder);
                }
                _ => debug!("key not found from: {}", holder),
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
            debug!(
                "get data with the max meta timestamp, from {} results",
                results.len()
            );
            Ok(Settings::choose_most_recent_data(results)
                .expect("results cannot be empty, because of the previous check"))
        }
    }

    async fn get_common(pearl: PearlHolder, key: BobKey) -> GetResult {
        let result = pearl.read(key).await.map(|data| BackendGetResult { data });
        if Error::is_get_error_need_restart(result.as_ref().err()) && pearl.try_reinit().await? {
            pearl.reinit_storage()?;
        }
        result
    }

    pub async fn exist(&self, keys: &[BobKey]) -> ExistResult {
        let mut exist = vec![false; keys.len()];
        let holders = self.pearls.read().await;
        for (ind, &key) in keys.iter().enumerate() {
            for holder in holders.iter() {
                exist[ind] = holder.pearl.exist(key).await.unwrap_or(false);
            }
        }
        Ok(BackendExistResult { exist })
    }

    pub fn pearls(&self) -> Arc<RwLock<Vec<PearlTimestampHolder>>> {
        self.pearls.clone()
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn disk_name(&self) -> &str {
        &self.disk_name
    }

    pub fn vdisk_id(&self) -> u32 {
        self.vdisk_id.as_u32()
    }

    pub async fn attach(&self, start_timestamp: i64) -> BackendResult<()> {
        let mut pearls = self.pearls.write().await;
        if pearls
            .iter()
            .all(|pearl| pearl.start_timestamp != start_timestamp)
        {
            let pearl_timestamp_holder = self.create_pearl_by_timestamp(start_timestamp);
            pearl_timestamp_holder.pearl.clone().prepare_storage().await;
            pearls.push(pearl_timestamp_holder);
            Ok(())
        } else {
            let msg = format!("pearl:{} already exists", start_timestamp);
            warn!("{}", msg);
            Err(Error::PearlChangeState(msg))
        }
    }
    pub async fn detach(&self, start_timestamp: i64) -> BackendResult<PearlTimestampHolder> {
        let mut pearls = self.pearls.write().await;
        debug!("write lock acquired");
        if let Some(pearl) = pearls.iter_mut().find(|pearl| {
            debug!("{}", pearl.start_timestamp);
            pearl.start_timestamp == start_timestamp
        }) {
            if self.settings.is_actual_pearl(&pearl) {
                let msg = format!(
                    "current active pearl:{} cannot be detached",
                    start_timestamp
                );
                warn!("{}", msg);
                Err(Error::PearlChangeState(msg))
            } else {
                let pearl: &mut PearlTimestampHolder = pearl;
                let pearl_holder: &mut PearlHolder = &mut pearl.pearl;
                let lock_guard: &LockGuard<PearlSync> = &pearl_holder.storage;
                let rwlock: &RwLock<PearlSync> = lock_guard.storage.as_ref();
                {
                    let pearl_sync: RwLockWriteGuard<_> = rwlock.write().await;
                    let storage: &Storage<_> = pearl_sync.storage.as_ref().expect("pearl storage");
                    if let Err(e) = storage.close().await {
                        warn!("pearl closed: {:?}", e);
                    }
                }
                let ret = pearl.clone();
                pearls.retain(|pearl| pearl.start_timestamp != start_timestamp);
                Ok(ret)
            }
        } else {
            let msg = format!("pearl:{} not found", start_timestamp);
            Err(Error::PearlChangeState(msg))
        }
    }

    pub fn create_pearl_timestamp_holder(&self, start_timestamp: i64) -> PearlTimestampHolder {
        let end_timestamp = start_timestamp + self.settings.get_timestamp_period();
        let mut path = self.directory_path.clone();
        path.push(format!("{}/", start_timestamp));

        PearlTimestampHolder::new(
            self.create_pearl_by_path(path),
            start_timestamp,
            end_timestamp,
        )
    }

    pub(crate) fn create_pearl_by_timestamp(&self, time: i64) -> PearlTimestampHolder {
        let start_timestamp =
            Stuff::get_start_timestamp_by_timestamp(self.settings.timestamp_period(), time);
        self.create_pearl_timestamp_holder(start_timestamp)
    }

    pub(crate) fn create_current_pearl(&self) -> PearlTimestampHolder {
        let start_timestamp = self.settings.get_current_timestamp_start();
        self.create_pearl_timestamp_holder(start_timestamp)
    }

    pub(crate) fn read_vdisk_directory(&self) -> BackendResult<Vec<PearlTimestampHolder>> {
        Stuff::check_or_create_directory(&self.directory_path)?;

        let mut pearls = vec![];
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
                let pearl_holder = self.create_pearl_timestamp_holder(start_timestamp);
                trace!("read pearl: {}", pearl_holder);
                pearls.push(pearl_holder);
            }
        }
        Ok(pearls)
    }
}

impl Display for PearlGroup {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        f.debug_struct("PearlGroup")
            .field("vdisk_id", &self.vdisk_id)
            .field("node_name", &self.node_name)
            .field("directory_path", &self.directory_path)
            .field("disk_name", &self.disk_name)
            .field("..", &"some fields ommited")
            .finish()
    }
}
