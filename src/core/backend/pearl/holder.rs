use crate::core::backend;
use crate::core::backend::pearl::{
    data::*,
    metrics::*,
    stuff::{LockGuard, Stuff},
};
use crate::core::configs::node::PearlConfig;
use crate::core::data::{BobData, BobKey, VDiskId};
use pearl::{Builder, ErrorKind, Storage};

use futures03::{
    compat::Future01CompatExt,
    future::err as err03,
    task::{Spawn, SpawnExt},
    FutureExt,
};

use std::{path::PathBuf, sync::Arc};
use tokio_timer::sleep;


#[derive(Clone)]
pub(crate) struct PearlHolder<TSpawner> {
    vdisk: VDiskId,
    disk_path: PathBuf,

    config: PearlConfig,
    spawner: TSpawner,

    pub(crate) storage: Arc<LockGuard<PearlSync>>,
}

impl<TSpawner: Spawn + Clone + Send + 'static + Unpin + Sync> PearlHolder<TSpawner> {
    pub fn new(
        vdisk: VDiskId,
        disk_path: PathBuf,
        config: PearlConfig,
        spawner: TSpawner,
    ) -> Self {
        PearlHolder {
            disk_path,
            vdisk,
            config,
            spawner,
            storage: Arc::new(LockGuard::new(PearlSync::new())),
        }
    }

    fn vdisk_print(&self) -> String {
        format!("{}", self.vdisk)
    }
    pub async fn update(&self, storage: Storage<PearlKey>) -> BackendResult<()> {
        trace!("try update Pearl id: {}", self.vdisk_print());

        self.storage
            .write_sync_mut(|st| {
                st.set(storage.clone());
                st.ready(); // current pearl disk is ready

                debug!(
                    "update Pearl id: {}, mark as ready, state: {}",
                    self.vdisk_print(),
                    st
                );
            })
            .await
    }

    pub async fn write(&self, key: BobKey, data: Box<BobData>) -> BackendResult<()> {
        self.storage
            .read(|st| {
                if !st.is_ready() {
                    trace!(
                        "Vdisk: {} is not ready for writing, state: {}",
                        self.vdisk_print(),
                        st
                    );
                    return err03(backend::Error::VDiskIsNotReady).boxed();
                }
                let storage = st.get();
                trace!("Vdisk: {}, write key: {}", self.vdisk_print(), key);
                Self::write_disk(storage, PearlKey::new(key), data.clone()).boxed()
            })
            .await
    }

    async fn write_disk(
        storage: PearlStorage,
        key: PearlKey,
        data: Box<BobData>,
    ) -> BackendResult<()> {
        PEARL_PUT_COUNTER.count(1);
        let timer = PEARL_PUT_TIMER.start();
        storage
            .write(key, PearlData::new(data).bytes())
            .await
            .map(|r| {
                PEARL_PUT_TIMER.stop(timer);
                r
            })
            .map_err(|e| {
                PEARL_PUT_ERROR_COUNTER.count(1);
                trace!("error on write: {:?}", e);
                //TODO check duplicate
                backend::Error::StorageError(format!("{:?}", e))
            })
    }

    pub async fn read(&self, key: BobKey) -> Result<BobData, backend::Error> {
        self.storage
            .read(|st| {
                if !st.is_ready() {
                    trace!(
                        "Vdisk: {} is not ready for reading, state: {}",
                        self.vdisk_print(),
                        st
                    );
                    return err03(backend::Error::VDiskIsNotReady).boxed();
                }
                let storage = st.get();
                trace!("Vdisk: {}, read key: {}", self.vdisk_print(), key);

                let q = async move {
                    PEARL_GET_COUNTER.count(1);
                    let timer = PEARL_GET_TIMER.start();
                    storage
                        .read(PearlKey::new(key))
                        .await
                        .map(|r| {
                            PEARL_GET_TIMER.stop(timer);
                            PearlData::parse(r)
                        })
                        .map_err(|e| {
                            PEARL_GET_ERROR_COUNTER.count(1);
                            trace!("error on read: {:?}", e);
                            match e.kind() {
                                ErrorKind::RecordNotFound => backend::Error::KeyNotFound,
                                _ => backend::Error::StorageError(format!("{:?}", e)),
                            }
                        })?
                };
                q.boxed()
            })
            .await
    }

    #[allow(dead_code)]
    async fn read_disk(storage: &PearlStorage, key: PearlKey) -> BackendResult<BobData> {
        PEARL_GET_COUNTER.count(1);
        let timer = PEARL_GET_TIMER.start();
        storage
            .read(key)
            .await
            .map(|r| {
                PEARL_GET_TIMER.stop(timer);
                PearlData::parse(r)
            })
            .map_err(|e| {
                PEARL_GET_ERROR_COUNTER.count(1);
                trace!("error on read: {:?}", e);
                match e.kind() {
                    ErrorKind::RecordNotFound => backend::Error::KeyNotFound,
                    _ => backend::Error::StorageError(format!("{:?}", e)),
                }
            })?
    }

    pub async fn try_reinit(&self) -> BackendResult<bool> {
        self.storage
            .write_mut(|st| {
                if st.is_reinit() {
                    trace!(
                        "Vdisk: {} is currently reinitializing, state: {}",
                        self.vdisk_print(),
                        st
                    );
                    return err03(backend::Error::VDiskIsNotReady).boxed();
                }
                st.init();
                trace!("Vdisk: {} set as reinit, state: {}", self.vdisk_print(), st);
                let storage = st.get();
                trace!("Vdisk: {} close old Pearl", self.vdisk_print());
                let q = async move {
                    let result = storage.close().await;
                    if let Err(e) = result {
                        error!("can't close pearl storage: {:?}", e);
                        return Ok(true); // we can't do anything
                    }
                    Ok(true)
                };

                q.boxed()
            })
            .await
    }

    pub async fn reinit_storage(self) -> BackendResult<()> {
        debug!("Vdisk: {} try reinit Pearl", self.vdisk_print());
        let mut spawner = self.spawner.clone();
        async move {
            debug!("Vdisk: {} start reinit Pearl", self.vdisk_print());
            let _ = spawner
                .spawn(self.prepare_storage().map(|_r| ()))
                .map_err(|e| {
                    error!("can't start reinit thread: {:?}", e);
                    panic!("can't start reinit thread: {:?}", e);
                });
        }
            .await;

        Ok(())
    }

    pub async fn prepare_storage(self) -> BackendResult<()> {
        let repeat = true;
        let path = &self.disk_path;
        let config = self.config.clone();
        // let spawner = self.spawner.clone();

        let delay = config.fail_retry_timeout();

        let mut need_delay = false;
        while repeat {
            if need_delay {
                let _ = sleep(delay).compat().boxed().await;
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
            if let Err(e) = self.update(st).await {
                error!("cannot update storage by path: {:?}, error: {:?}", path, e);
                //TODO drop storage  .Part 2: i think we should panic here
                continue;
            }
            debug!("Vdisk: {} Pearl is ready for work", self.vdisk_print());
            return Ok(());
        }
        Err(backend::Error::StorageError("stub".to_string()))
    }

    fn init_pearl_by_path(path: &PathBuf, config: &PearlConfig) -> BackendResult<PearlStorage> {
        let mut builder = Builder::new().work_dir(path.clone());

        builder = match &config.blob_file_name_prefix {
            Some(blob_file_name_prefix) => builder.blob_file_name_prefix(blob_file_name_prefix),
            _ => builder.blob_file_name_prefix("bob"),
        };
        builder = match config.max_data_in_blob {
            Some(max_data_in_blob) => builder.max_data_in_blob(max_data_in_blob),
            _ => builder,
        };
        builder = match config.max_blob_size {
            Some(max_blob_size) => builder.max_blob_size(max_blob_size),
            _ => panic!("'max_blob_size' is not set in pearl config"),
        };

        let storage = builder
            .build()
            .map_err(|e| backend::Error::StorageError(format!("{:?}", e)));
        if let Err(e) = storage {
            error!("cannot build pearl by path: {:?}, error: {}", path, e);
            return Err(backend::Error::StorageError(format!(
                "cannot build pearl by path: {:?}, error: {}",
                path, e
            )));
        }
        trace!("Pearl is created by path: {:?}", path);
        storage
    }

    #[allow(dead_code)]
    pub(crate) async fn test<TRet, F>(&self, f: F) -> BackendResult<TRet>
    where
        F: Fn(&mut PearlSync) -> TRet + Send + Sync,
    {
        self.storage.write_sync_mut(|st| f(st)).await
    }
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum PearlState {
    Normal,       // pearl is started and working
    Initializing, // pearl restarting
}

#[derive(Clone)]
pub(crate) struct PearlSync {
    pub(crate) storage: Option<PearlStorage>,
    state: PearlState,

    pub(crate) start_time_test: u8,
}
impl PearlSync {
    pub(crate) fn new() -> Self {
        PearlSync {
            storage: None,
            state: PearlState::Initializing,
            start_time_test: 0,
        }
    }
    pub(crate) fn ready(&mut self) {
        self.set_state(PearlState::Normal);
    }
    pub(crate) fn init(&mut self) {
        self.set_state(PearlState::Initializing);
    }
    pub(crate) fn is_ready(&self) -> bool {
        self.get_state() == PearlState::Normal
    }
    pub(crate) fn is_reinit(&self) -> bool {
        self.get_state() == PearlState::Initializing
    }

    pub(crate) fn set_state(&mut self, state: PearlState) {
        self.state = state;
    }

    pub(crate) fn get_state(&self) -> PearlState {
        self.state.clone()
    }

    pub(crate) fn set(&mut self, storage: PearlStorage) {
        self.storage = Some(storage);
        self.start_time_test += 1;
    }
    pub(crate) fn get(&self) -> PearlStorage {
        self.storage.clone().unwrap()
    }
}

impl std::fmt::Display for PearlSync {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "#{:?}", self.state)
    }
}