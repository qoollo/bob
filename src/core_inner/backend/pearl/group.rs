use super::prelude::*;

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
            match self.create_current_pearl() {
                Ok(current_pearl) => {
                    debug!("{}: create current pearl: {}", self, current_pearl);
                    pearls.push(current_pearl);
                }
                Err(e) => {
                    debug!("{}: cannot create current pearl: {}", self, e);
                    //we will try again when some data come for put\get
                }
            }
        }

        debug!("{}: save pearls to group", self);
        while let Err(err) = self.add_range(pearls.clone()).await {
            error!("{}: can't add pearls: {:?}", self, err);
            delay_for(t).await;
        }

        debug!("{}: start pearls", self);
        while let Err(err) = self.run_pearls().await {
            error!("{}: can't start pearls: {:?}", self, err);
            delay_for(t).await;
        }
    }

    async fn run_pearls(&self) -> BackendResult<()> {
        let holders = self.pearls.write().compat().await.map_err(|e| {
            error!("{}: cannot take lock: {:?}", self, e);
            Error::Failed(format!("cannot take lock: {:?}", e))
        })?;

        for holder in holders.iter() {
            let pearl = holder.pearl.clone();
            pearl.prepare_storage().await?;
        }
        Ok(())
    }

    pub fn create_pearl_by_path(&self, path: PathBuf) -> PearlHolder {
        PearlHolder::new(self.vdisk_id.clone(), path, self.config.clone())
    }

    pub async fn add(&self, pearl: PearlTimestampHolder) -> BackendResult<()> {
        self.pearls
            .write()
            .compat()
            .await
            .map(|mut pearls| {
                pearls.push(pearl);
            })
            .map_err(|e| {
                error!("cannot take lock: {:?}", e);
                Error::Failed(format!("cannot take lock: {:?}", e))
            })
    }

    pub async fn add_range(&self, new_pearls: Vec<PearlTimestampHolder>) -> BackendResult<()> {
        self.pearls
            .write()
            .compat()
            .await
            .map(|mut pearls| {
                pearls.extend(new_pearls);
            })
            .map_err(|e| {
                error!("cannot take lock: {:?}", e);
                Error::Failed(format!("cannot take lock: {:?}", e))
            })
    }

    /// find in all pearls actual pearl and try create new
    async fn try_get_current_pearl(&self, data: &BobData) -> BackendResult<PearlTimestampHolder> {
        let task = self.find_current_pearl(data).or_else(|e| {
            debug!("cannot find pearl: {}", e);
            self.create_current_write_pearl(data)
                .and_then(|_| self.find_current_pearl(data))
        });
        task.await
    }

    /// find in all pearls actual pearl
    async fn find_current_pearl(&self, data: &BobData) -> BackendResult<PearlTimestampHolder> {
        self.pearls
            .read()
            .compat()
            .await
            .map_err(|e| {
                error!("cannot take lock: {:?}", e);
                Error::Failed(format!("cannot take lock: {:?}", e))
            })
            .and_then(|pearls| {
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
            })
    }

    /// create pearl for current write
    async fn create_current_write_pearl(&self, data: &BobData) -> BackendResult<()> {
        // check if pearl is currently creating
        if self.pearl_sync.try_init().await? {
            // check if pearl created
            if self.find_current_pearl(&data).await.is_err() {
                match self.create_pearl_by_timestamp(data.meta.timestamp) {
                    Ok(pearl) => self.save_pearl(pearl).await,
                    Err(e) => Err(e),
                }?;
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
        self.add(holder).await?; // TODO while retry?
        pearl.prepare_storage().await
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
        let holders = self.pearls.read().compat().await.map_err(|e| {
            error!("cannot take lock: {:?}", e);
            Error::Failed(format!("cannot take lock: {:?}", e))
        })?;

        let mut has_error = false;
        let mut results = vec![];
        for holder in holders.iter() {
            let get = Self::get_common(holder.pearl.clone(), key).await;
            match get {
                Ok(data) => {
                    trace!("get data: {} from: {}", data, holder);
                    results.push(data);
                }
                Err(err) if err != backend::Error::KeyNotFound => {
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
                Err(Error::KeyNotFound)
            }
        } else {
            Settings::choose_data(results)
        }
    }

    async fn get_common(pearl: PearlHolder, key: BobKey) -> GetResult {
        let result = pearl.read(key).await.map(|data| BackendGetResult { data });
        if Error::is_get_error_need_restart(result.as_ref().err()) && pearl.try_reinit().await? {
            pearl.reinit_storage()?;
        }
        result
    }

    pub fn pearls(&self) -> Option<RwLockReadGuard<Vec<PearlTimestampHolder>>> {
        self.pearls.try_read().ok()
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

    pub fn create_pearl_timestamp_holder(
        &self,
        start_timestamp: i64,
    ) -> BackendResult<PearlTimestampHolder> {
        let end_timestamp = start_timestamp + self.settings.get_timestamp_period()?;
        let mut path = self.directory_path.clone();
        path.push(format!("{}/", start_timestamp));

        Ok(PearlTimestampHolder::new(
            self.create_pearl_by_path(path),
            start_timestamp,
            end_timestamp,
        ))
    }

    pub(crate) fn create_pearl_by_timestamp(
        &self,
        time: i64,
    ) -> BackendResult<PearlTimestampHolder> {
        let start_timestamp =
            Stuff::get_start_timestamp_by_timestamp(self.settings.timestamp_period(), time)?;
        self.create_pearl_timestamp_holder(start_timestamp)
    }

    pub(crate) fn create_current_pearl(&self) -> BackendResult<PearlTimestampHolder> {
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
                let pearl_holder = self.create_pearl_timestamp_holder(start_timestamp)?;
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
