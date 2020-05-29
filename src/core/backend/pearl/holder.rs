use super::prelude::*;

/// Struct hold pearl and add put/get/restart api
#[derive(Clone, Debug)]
pub(crate) struct Holder {
    start_timestamp: u64,
    end_timestamp: u64,
    vdisk: VDiskId,
    disk_path: PathBuf,
    config: PearlConfig,
    storage: Arc<RwLock<PearlSync>>,
}

impl Holder {
    pub(crate) fn new(
        start_timestamp: u64,
        end_timestamp: u64,
        vdisk: VDiskId,
        disk_path: PathBuf,
        config: PearlConfig,
    ) -> Self {
        Self {
            start_timestamp,
            end_timestamp,
            vdisk,
            disk_path,
            config,
            storage: Arc::new(RwLock::new(PearlSync::new())),
        }
    }

    pub(crate) fn start_timestamp(&self) -> u64 {
        self.start_timestamp
    }

    pub(crate) fn storage(&self) -> &RwLock<PearlSync> {
        &self.storage
    }

    pub(crate) fn is_actual(&self, current_start: u64) -> bool {
        self.start_timestamp == current_start
    }

    pub(crate) async fn records_count(&self) -> usize {
        let storage = self.storage.read().await;
        storage.records_count().await
    }

    pub(crate) fn gets_into_interval(&self, timestamp: u64) -> bool {
        self.start_timestamp <= timestamp && timestamp < self.end_timestamp
    }

    pub async fn update(&self, storage: Storage<Key>) {
        let mut st = self.storage.write().await;
        st.set(storage.clone());
        st.ready(); // current pearl disk is ready
        debug!(
            "update Pearl id: {}, mark as ready, state: {:?}",
            self.vdisk, st
        );
    }

    pub async fn write(&self, key: BobKey, data: BobData) -> BackendResult<()> {
        let state = self.storage.read().await;

        if state.is_ready() {
            let storage = state.get();
            trace!("Vdisk: {}, write key: {}", self.vdisk, key);
            Self::write_disk(storage, Key::from(key), data.clone()).await
        } else {
            trace!("Vdisk: {} isn't ready for writing: {:?}", self.vdisk, state);
            Err(Error::VDiskIsNotReady)
        }
    }

    // @TODO remove redundant return result
    async fn write_disk(storage: PearlStorage, key: Key, data: BobData) -> BackendResult<()> {
        PEARL_PUT_COUNTER.count(1);
        let timer = PEARL_PUT_TIMER.start();
        storage
            .write(key, Data::from(data).to_vec())
            .await
            .unwrap_or_else(|e| {
                PEARL_PUT_ERROR_COUNTER.count(1);
                error!("error on write: {:?}", e);
                //TODO check duplicate
            });
        PEARL_PUT_TIMER.stop(timer);
        Ok(())
    }

    pub async fn read(&self, key: BobKey) -> Result<BobData, Error> {
        let state = self.storage.read().await;
        if state.is_ready() {
            let storage = state.get();
            trace!("Vdisk: {}, read key: {}", self.vdisk, key);
            PEARL_GET_COUNTER.count(1);
            let timer = PEARL_GET_TIMER.start();
            storage
                .read(Key::from(key))
                .await
                .map(|r| {
                    PEARL_GET_TIMER.stop(timer);
                    Data::from_bytes(&r)
                })
                .map_err(|e| {
                    PEARL_GET_ERROR_COUNTER.count(1);
                    trace!("error on read: {:?}", e);
                    match e.kind() {
                        ErrorKind::RecordNotFound => Error::KeyNotFound(key),
                        _ => Error::Storage(format!("{:?}", e)),
                    }
                })?
        } else {
            trace!("Vdisk: {} isn't ready for reading: {:?}", self.vdisk, state);
            Err(Error::VDiskIsNotReady)
        }
    }

    pub async fn try_reinit(&self) -> BackendResult<()> {
        let mut state = self.storage.write().await;
        if state.is_reinit() {
            trace!(
                "Vdisk: {} reinitializing now, state: {:?}",
                self.vdisk,
                state
            );
            Err(Error::VDiskIsNotReady)
        } else {
            state.init();
            trace!("Vdisk: {} set as reinit, state: {:?}", self.vdisk, state);
            let storage = state.get();
            trace!("Vdisk: {} close old Pearl", self.vdisk);
            let result = storage.close().await;
            if let Err(e) = result {
                error!("can't close pearl storage: {:?}", e);
                // we can't do anything
            }
            Ok(())
        }
    }

    pub async fn exist(&self, key: BobKey) -> Result<bool, Error> {
        let state = self.storage.read().await;
        if state.is_ready() {
            trace!("Vdisk: {}, check key: {}", self.vdisk, key);
            let pearl_key = Key::from(key);
            let storage = state.get();
            Ok(storage.contains(pearl_key).await)
        } else {
            trace!("Vdisk: {} not ready for reading: {:?}", self.vdisk, state);
            Err(Error::VDiskIsNotReady)
        }
    }

    pub async fn prepare_storage(&self) -> Result<(), Error> {
        self.config
            .try_multiple_times_async(
                || self.init_holder(),
                "can't initialize holder",
                self.config.fail_retry_timeout(),
            )
            .await
    }

    async fn init_holder(&self) -> Result<(), Error> {
        self.config
            .try_multiple_times(
                || Stuff::check_or_create_directory(&self.disk_path),
                &format!("cannot check path: {:?}", self.disk_path),
                self.config.fail_retry_timeout(),
            )
            .await?;

        self.config
            .try_multiple_times(
                || Stuff::drop_pearl_lock_file(&self.disk_path),
                &format!("cannot delete lock file: {:?}", self.disk_path),
                self.config.fail_retry_timeout(),
            )
            .await?;

        let storage = self
            .config
            .try_multiple_times(
                || self.init_pearl_by_path(),
                &format!("can't init pearl by path: {:?}", self.disk_path),
                self.config.fail_retry_timeout(),
            )
            .await?;
        self.init_pearl(storage).await?;
        debug!("Vdisk: {} Pearl is ready for work", self.vdisk);
        Ok(())
    }

    async fn init_pearl(&self, mut storage: Storage<Key>) -> Result<(), Error> {
        match storage.init().await {
            Ok(_) => {
                self.update(storage).await;
                Ok(())
            }
            Err(e) => Err(Error::Storage(format!("pearl error: {:?}", e))),
        }
    }

    pub(crate) fn drop_directory(&self) -> BackendResult<()> {
        Stuff::drop_directory(&self.disk_path)
    }

    fn init_pearl_by_path(&self) -> BackendResult<PearlStorage> {
        let mut builder = Builder::new().work_dir(&self.disk_path);

        if self.config.allow_duplicates() {
            builder = builder.allow_duplicates();
        }

        // @TODO add default values to be inserted on deserialisation step
        let prefix = self.config.blob_file_name_prefix();
        let max_data = self.config.max_data_in_blob();
        let max_blob_size = self.config.max_blob_size();
        builder
            .blob_file_name_prefix(prefix)
            .max_data_in_blob(max_data)
            .max_blob_size(max_blob_size)
            .set_filter_config(BloomConfig::default())
            .build()
            .map_err(|e| {
                error!("cannot build pearl by path: {:?}, {}", &self.disk_path, e);
                Error::Storage(e.to_string())
            })
    }
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum PearlState {
    // pearl is started and working
    Normal,
    // pearl restarting
    Initializing,
}

#[derive(Clone, Debug)]
pub(crate) struct PearlSync {
    storage: Option<PearlStorage>,
    state: PearlState,
    start_time_test: u8,
}
impl PearlSync {
    pub(crate) fn new() -> Self {
        Self {
            storage: None,
            state: PearlState::Initializing,
            start_time_test: 0,
        }
    }

    pub(crate) fn storage(&self) -> &PearlStorage {
        self.storage.as_ref().expect("pearl storage")
    }

    pub(crate) async fn records_count(&self) -> usize {
        self.storage().records_count().await
    }

    #[inline]
    pub(crate) fn ready(&mut self) {
        self.set_state(PearlState::Normal);
    }

    #[inline]
    pub(crate) fn init(&mut self) {
        self.set_state(PearlState::Initializing);
    }

    #[inline]
    pub(crate) fn is_ready(&self) -> bool {
        self.state == PearlState::Normal
    }

    #[inline]
    pub(crate) fn is_reinit(&self) -> bool {
        self.state == PearlState::Initializing
    }

    #[inline]
    pub(crate) fn set_state(&mut self, state: PearlState) {
        self.state = state;
    }

    #[inline]
    pub(crate) fn set(&mut self, storage: PearlStorage) {
        self.storage = Some(storage);
        self.start_time_test += 1;
    }

    #[inline]
    pub(crate) fn get(&self) -> PearlStorage {
        self.storage.clone().expect("cloned storage")
    }
}
