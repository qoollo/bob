use super::prelude::*;

/// Struct hold pearl and add put/get/restart api
#[derive(Clone, Debug)]
pub(crate) struct Holder {
    start_timestamp: i64,
    end_timestamp: i64,
    vdisk: VDiskId,
    disk_path: PathBuf,
    config: PearlConfig,
    pub(crate) storage: Arc<LockGuard<PearlSync>>,
}

impl Holder {
    pub(crate) fn new(
        start_timestamp: i64,
        end_timestamp: i64,
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
            storage: Arc::new(LockGuard::new(PearlSync::new())),
        }
    }

    pub(crate) fn start_timestamp(&self) -> i64 {
        self.start_timestamp
    }

    pub(crate) fn end_timestamp(&self) -> i64 {
        self.end_timestamp
    }

    pub(crate) fn is_actual(&self, current_start: i64) -> bool {
        self.start_timestamp() == current_start
    }

    pub async fn update(&self, storage: Storage<Key>) {
        trace!("try update Pearl id: {}", self.vdisk);
        self.storage
            .write_sync_mut(|st| {
                st.set(storage.clone());
                st.ready(); // current pearl disk is ready
                debug!(
                    "update Pearl id: {}, mark as ready, state: {}",
                    self.vdisk, st
                );
            })
            .await;
    }

    pub async fn write(&self, key: BobKey, data: BobData) -> BackendResult<()> {
        let task = self.storage.read(|state| {
            if state.is_ready() {
                let storage = state.get();
                trace!("Vdisk: {}, write key: {}", self.vdisk, key);
                Self::write_disk(storage, Key::from(key), data.clone()).boxed()
            } else {
                trace!(
                    "Vdisk: {} is not ready for writing, state: {}",
                    self.vdisk,
                    state
                );
                future::err(Error::VDiskIsNotReady).boxed()
            }
        });
        task.await
    }

    async fn write_disk(storage: PearlStorage, key: Key, data: BobData) -> BackendResult<()> {
        PEARL_PUT_COUNTER.count(1);
        let timer = PEARL_PUT_TIMER.start();
        storage
            .write(key, Data::from(data).to_vec())
            .await
            .unwrap_or_else(|e| {
                PEARL_PUT_ERROR_COUNTER.count(1);
                trace!("error on write: {:?}", e);
                //TODO check duplicate
            });
        PEARL_PUT_TIMER.stop(timer);
        Ok(())
    }

    pub async fn read(&self, key: BobKey) -> Result<BobData, Error> {
        self.storage
            .read(|state| {
                if state.is_ready() {
                    let storage = state.get();
                    trace!("Vdisk: {}, read key: {}", self.vdisk, key);
                    PEARL_GET_COUNTER.count(1);
                    let q = async move {
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
                    };
                    q.boxed()
                } else {
                    trace!(
                        "Vdisk: {} is not ready for reading, state: {}",
                        self.vdisk,
                        state
                    );
                    future::err(Error::VDiskIsNotReady).boxed()
                }
            })
            .await
    }

    pub async fn try_reinit(&self) -> BackendResult<bool> {
        self.storage
            .write_mut(|state| {
                if state.is_reinit() {
                    trace!(
                        "Vdisk: {} is currently reinitializing, state: {}",
                        self.vdisk,
                        state
                    );
                    future::err(Error::VDiskIsNotReady).boxed()
                } else {
                    state.init();
                    trace!("Vdisk: {} set as reinit, state: {}", self.vdisk, state);
                    let storage = state.get();
                    trace!("Vdisk: {} close old Pearl", self.vdisk);
                    let q = async move {
                        let result = storage.close().await;
                        if let Err(e) = result {
                            error!("can't close pearl storage: {:?}", e);
                            // we can't do anything
                        }
                        Ok(true)
                    };
                    q.boxed()
                }
            })
            .await
    }

    pub async fn exist(&self, key: BobKey) -> Result<bool, Error> {
        let state = self.storage.storage.read().await;
        if state.is_ready() {
            trace!("Vdisk: {}, check key: {}", self.vdisk, key);
            let pearl_key = Key::from(key);
            let storage = state.get();
            Ok(storage.contains(pearl_key).await)
        } else {
            trace!(
                "Vdisk: {} is not ready for reading, state: {:?}",
                self.vdisk,
                state
            );
            Err(Error::VDiskIsNotReady)
        }
    }

    pub fn reinit_storage(self) -> BackendResult<()> {
        debug!("Vdisk: {} try reinit Pearl", self.vdisk);
        tokio::spawn(async move { self.prepare_storage().await });
        Ok(())
    }

    pub async fn prepare_storage(&self) {
        let path = &self.disk_path;
        let config = self.config.clone();
        let t = config.fail_retry_timeout();

        let mut need_delay = false;
        loop {
            if need_delay {
                delay_for(t).await;
            }
            need_delay = true;

            if let Err(e) = Stuff::check_or_create_directory(path) {
                error!("cannot check path: {:?}, error: {}", path, e);
                continue;
            }

            if let Err(e) = Stuff::drop_pearl_lock_file(path) {
                error!("cannot delete lock file: {:?}, error: {}", path, e);
                continue;
            }

            let storage = Self::init_pearl_by_path(path, &config);
            if let Err(e) = storage {
                error!("cannot build pearl by path: {:?}, error: {:?}", path, e);
                continue;
            }
            let mut st = storage.unwrap();
            if let Err(e) = st.init().await {
                error!("cannot init pearl by path: {:?}, error: {:?}", path, e);
                continue;
            }
            self.update(st).await;
            debug!("Vdisk: {} Pearl is ready for work", self.vdisk);
            break;
        }
    }

    pub(crate) fn drop_directory(&self) -> BackendResult<()> {
        Stuff::drop_directory(&self.disk_path)
    }

    fn init_pearl_by_path(path: &PathBuf, config: &PearlConfig) -> BackendResult<PearlStorage> {
        let mut builder = Builder::new().work_dir(path);

        if config.allow_duplicates.unwrap_or(true) {
            builder = builder.allow_duplicates();
        }

        let prefix = config
            .blob_file_name_prefix
            .clone()
            .unwrap_or_else(|| "bob".to_string());
        let max_data = config
            .max_data_in_blob
            .expect("max_data_in_blob is not set in pearl config");
        let max_blob_size = config
            .max_blob_size
            .expect("'max_blob_size' is not set in pearl config");
        builder
            .blob_file_name_prefix(prefix)
            .max_data_in_blob(max_data)
            .max_blob_size(max_blob_size)
            .build()
            .map_err(|e| {
                error!("cannot build pearl by path: {:?}, error: {}", path, e);
                Error::Storage(e.to_string())
            })
    }
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum PearlState {
    /// pearl is started and working
    Normal,
    /// pearl restarting
    Initializing,
}

#[derive(Clone, Debug)]
pub(crate) struct PearlSync {
    pub(crate) storage: Option<PearlStorage>,
    state: PearlState,

    pub(crate) start_time_test: u8,
}
impl PearlSync {
    pub(crate) fn new() -> Self {
        Self {
            storage: None,
            state: PearlState::Initializing,
            start_time_test: 0,
        }
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
        self.get_state() == PearlState::Normal
    }

    #[inline]
    pub(crate) fn is_reinit(&self) -> bool {
        self.get_state() == PearlState::Initializing
    }

    #[inline]
    pub(crate) fn set_state(&mut self, state: PearlState) {
        self.state = state;
    }

    #[inline]
    pub(crate) fn get_state(&self) -> PearlState {
        self.state.clone()
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

impl std::fmt::Display for PearlSync {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("PearlSync")
            .field("state", &self.state)
            .field("..", &"some fields ommited")
            .finish()
    }
}
