use std::time::UNIX_EPOCH;

use super::prelude::*;

const MAX_TIME_SINCE_LAST_WRITE_SEC: u64 = 10;
const SMALL_RECORDS_COUNT_MUL: u64 = 10;

/// Struct hold pearl and add put/get/restart api
#[derive(Clone, Debug)]
pub(crate) struct Holder {
    start_timestamp: u64,
    end_timestamp: u64,
    vdisk: VDiskID,
    disk_path: PathBuf,
    config: PearlConfig,
    storage: Arc<RwLock<PearlSync>>,
    last_write_ts: Arc<RwLock<u64>>,
}

impl Holder {
    pub(crate) fn new(
        start_timestamp: u64,
        end_timestamp: u64,
        vdisk: VDiskID,
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
            last_write_ts: Arc::new(RwLock::new(0)),
        }
    }

    pub(crate) fn start_timestamp(&self) -> u64 {
        self.start_timestamp
    }

    pub(crate) fn end_timestamp(&self) -> u64 {
        self.end_timestamp
    }

    pub(crate) fn get_id(&self) -> String {
        self.disk_path
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .unwrap_or("unparsable string")
            .to_owned()
    }

    pub(crate) fn storage(&self) -> &RwLock<PearlSync> {
        &self.storage
    }

    pub(crate) async fn blobs_count(&self) -> usize {
        let storage = self.storage.read().await;
        storage.blobs_count().await
    }

    pub(crate) async fn index_memory(&self) -> usize {
        let storage = self.storage.read().await;
        storage.index_memory().await
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

    pub(crate) fn is_outdated(&self) -> bool {
        let ts = Self::get_current_ts();
        ts > self.end_timestamp
    }

    pub(crate) async fn no_writes_recently(&self) -> bool {
        let ts = Self::get_current_ts();
        let last_write_ts = *self.last_write_ts.read().await;
        ts - last_write_ts > MAX_TIME_SINCE_LAST_WRITE_SEC
    }

    pub(crate) async fn active_blob_is_empty(&self) -> bool {
        let active = self
            .storage()
            .read()
            .await
            .active_blob_records_count()
            .await as u64;
        active == 0
    }

    pub(crate) async fn active_blob_is_small(&self) -> bool {
        let active = self
            .storage()
            .read()
            .await
            .active_blob_records_count()
            .await as u64;
        active * SMALL_RECORDS_COUNT_MUL < self.config.max_data_in_blob()
    }

    fn get_current_ts() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("current time is before unix epoch")
            .as_secs()
    }

    pub(crate) async fn close_active_blob(&mut self) {
        let storage = self.storage.write().await;
        storage.storage().close_active_blob().await;
        warn!("Active blob of {} closed", self.get_id());
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
            *self.last_write_ts.write().await = Self::get_current_ts();
            trace!("Vdisk: {}, write key: {}", self.vdisk, key);
            Self::write_disk(storage, Key::from(key), data.clone()).await
        } else {
            trace!("Vdisk: {} isn't ready for writing: {:?}", self.vdisk, state);
            Err(Error::vdisk_is_not_ready())
        }
    }

    // @TODO remove redundant return result
    #[allow(clippy::cast_possible_truncation)]
    async fn write_disk(storage: PearlStorage, key: Key, data: BobData) -> BackendResult<()> {
        counter!(PEARL_PUT_COUNTER, 1);
        let timer = Instant::now();
        storage
            .write(key, Data::from(data).to_vec())
            .await
            .unwrap_or_else(|e| {
                counter!(PEARL_PUT_ERROR_COUNTER, 1);
                error!("error on write: {:?}", e);
                //TODO check duplicate
            });
        counter!(PEARL_PUT_TIMER, timer.elapsed().as_nanos() as u64);
        Ok(())
    }

    #[allow(clippy::cast_possible_truncation)]
    pub async fn read(&self, key: BobKey) -> Result<BobData, Error> {
        let state = self.storage.read().await;
        if state.is_ready() {
            let storage = state.get();
            trace!("Vdisk: {}, read key: {}", self.vdisk, key);
            counter!(PEARL_GET_COUNTER, 1);
            let timer = Instant::now();
            let res = storage
                .read(Key::from(key))
                .await
                .map(|r| Data::from_bytes(&r))
                .map_err(|e| {
                    counter!(PEARL_GET_ERROR_COUNTER, 1);
                    trace!("error on read: {:?}", e);
                    match e.downcast_ref::<PearlError>().unwrap().kind() {
                        ErrorKind::RecordNotFound => Error::key_not_found(key),
                        _ => Error::storage(e.to_string()),
                    }
                });
            counter!(PEARL_GET_TIMER, timer.elapsed().as_nanos() as u64);
            res?
        } else {
            trace!("Vdisk: {} isn't ready for reading: {:?}", self.vdisk, state);
            Err(Error::vdisk_is_not_ready())
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
            Err(Error::vdisk_is_not_ready())
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
            storage.contains(pearl_key).await.map_err(|e| {
                error!("{}", e);
                Error::internal()
            })
        } else {
            trace!("Vdisk: {} not ready for reading: {:?}", self.vdisk, state);
            Err(Error::vdisk_is_not_ready())
        }
    }

    pub async fn prepare_storage(&self) -> Result<()> {
        debug!("backend pearl holder prepare storage");
        self.config
            .try_multiple_times_async(
                || self.init_holder(),
                "can't initialize holder",
                self.config.fail_retry_timeout(),
            )
            .await
    }

    async fn init_holder(&self) -> Result<()> {
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
            .await
            .with_context(|| "backend pearl holder init storage failed")?;
        self.init_pearl(storage).await?;
        debug!("backend pearl holder init holder ready #{}", self.vdisk);
        Ok(())
    }

    async fn init_pearl(&self, mut storage: Storage<Key>) -> Result<(), Error> {
        match storage.init().await {
            Ok(_) => {
                self.update(storage).await;
                Ok(())
            }
            Err(e) => Err(Error::storage(format!("pearl error: {:?}", e))),
        }
    }

    pub(crate) fn drop_directory(&self) -> BackendResult<()> {
        Stuff::drop_directory(&self.disk_path)
    }

    fn init_pearl_by_path(&self) -> Result<PearlStorage> {
        let mut builder = Builder::new().work_dir(&self.disk_path);

        if self.config.allow_duplicates() {
            builder = builder.allow_duplicates();
        }

        // @TODO add default values to be inserted on deserialisation step
        let prefix = self.config.blob_file_name_prefix();
        let max_data = self.config.max_data_in_blob();
        let max_blob_size = self.config.max_blob_size();
        let builder = builder
            .blob_file_name_prefix(prefix)
            .max_data_in_blob(max_data)
            .max_blob_size(max_blob_size)
            .set_filter_config(BloomConfig::default());
        let builder = if self.config.is_aio_enabled() {
            match rio::new() {
                Ok(ioring) => {
                    warn!("bob will start with AIO - async fs io api");
                    builder.enable_aio(ioring)
                }
                Err(e) => {
                    warn!("bob will start with standard sync fs io api");
                    warn!("can't start with AIO, cause: {}", e);
                    builder
                }
            }
        } else {
            warn!("bob will start with standard sync fs io api");
            warn!("cause: disabled in config");
            builder
        };
        builder
            .build()
            .with_context(|| format!("cannot build pearl by path: {:?}", &self.disk_path))
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

    pub(crate) async fn index_memory(&self) -> usize {
        self.storage().index_memory().await
    }

    pub(crate) async fn active_blob_records_count(&self) -> usize {
        self.storage()
            .records_count_in_active_blob()
            .await
            .unwrap_or_default()
    }

    pub(crate) async fn blobs_count(&self) -> usize {
        self.storage().blobs_count().await
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
