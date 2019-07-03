use crate::core::backend::core::*;
use crate::core::backend;
use crate::core::backend::pearl::{data::*, stuff::{LockGuard, Stuff}};
use crate::core::configs::node::{NodeConfig, PearlConfig};
use crate::core::data::{BobData, BobKey, VDiskId, VDiskMapper};
use pearl::{Builder, Storage, ErrorKind};

use futures03::{
    future::err as err03,
    task::{Spawn, SpawnExt},
    FutureExt,
};

use std::{path::PathBuf, sync::Arc};

pub struct PearlBackend<TSpawner> {
    vdisks: Arc<Vec<PearlVDisk<TSpawner>>>,
    alien_dir: PearlVDisk<TSpawner>,
}

impl<TSpawner: Spawn + Clone + Send + 'static + Unpin + Sync> PearlBackend<TSpawner> {
    pub fn new(mapper: VDiskMapper, config: &NodeConfig, spawner: TSpawner) -> Self {
        debug!("initializing pearl backend");
        let pearl_config = config.pearl.clone().unwrap();

        let mut result = Vec::new();

        //init pearl storages for each vdisk
        for disk in mapper.local_disks().iter() {
            let base_path = PathBuf::from(format!("{}/bob/", disk.path));
            while let Err(e) = Stuff::check_or_create_directory(&base_path) {
                error!(
                    "disk: {}, cannot check path: {:?}, error: {}",
                    disk, base_path, e
                );
                //TODO sleep. use config params. Part 2: make it async? Part 3 : and in separated thread
            }
            let mut vdisks: Vec<PearlVDisk<TSpawner>> = mapper
                .get_vdisks_by_disk(&disk.name)
                .iter()
                .map(|vdisk_id| {
                    let mut vdisk_path = base_path.clone();
                    vdisk_path.push(format!("{}/", vdisk_id));
                    
                    PearlVDisk::new(
                        &disk.path,
                        &disk.name,
                        vdisk_id.clone(),
                        vdisk_path,
                        pearl_config.clone(),
                        spawner.clone(),
                    )
                })
                .collect();
            result.append(&mut vdisks);
        }

        //init alien storage
        let path = format!(
            "{}/alien/",
            mapper
                .get_disk_by_name(&pearl_config.alien_disk())
                .unwrap()
                .path
        );

        let alien_path = PathBuf::from(path.clone());
        let alien_dir = PearlVDisk::new_alien(
            &path,
            &pearl_config.alien_disk(),
            alien_path,
            pearl_config.clone(),
            spawner.clone(),
        );

        PearlBackend {
            vdisks: Arc::new(result),
            alien_dir: alien_dir,
        }
    }
}

impl<TSpawner: Spawn + Clone + Send + 'static + Unpin + Sync> BackendStorage
    for PearlBackend<TSpawner>
{
    fn run_backend(&self) -> RunResult {
        debug!("run pearl backend");

        let vdisks = self.vdisks.clone();
        let alien_dir = self.alien_dir.clone();
        let q = async move {
            for i in 0..vdisks.len() {
                let _ = vdisks[i]
                    .clone()
                    .prepare_storage() //TODO add Box?
                    .await;
            }

            let _ = alien_dir.prepare_storage().await;
            Ok(())
        };
        

        q.boxed()
    }

    fn put(&self, disk_name: String, vdisk_id: VDiskId, key: BobKey, data: BobData) -> Put {
        debug!("PUT[{}][{}][{}] to pearl backend", disk_name, vdisk_id, key);

        let vdisks = self.vdisks.clone();
        Put({
            let vdisk = vdisks.iter().find(|vd| vd.equal(&disk_name, &vdisk_id));
            if let Some(disk) = vdisk {
                let d_clone = disk.clone(); // TODO remove copy of disk. add Box?
                async move {
                    let result = d_clone
                        .write(key, Box::new(data))
                        .map(|r| {
                            r.map(|_ok| BackendPutResult {}).map_err(|e| {
                                debug!(
                                    "PUT[{}][{}][{}], error: {:?}",
                                    disk_name, vdisk_id, key, e
                                );
                                e
                            })
                        })
                        .await;
                    if result.is_err() && d_clone.try_reinit().await.unwrap() { //TODO check err type
                        //TODO panic if failed lock?
                        let _ = d_clone.reinit_storage().await;
                    }
                    result
                }
                    .boxed()
            }
            else {
                debug!(
                    "PUT[{}][{}][{}] to pearl backend. Cannot find storage",
                    disk_name, vdisk_id, key
                );
                err03(backend::Error::VDiskNoFound(vdisk_id)).boxed()
            }
        })
    }

    fn put_alien(&self, _vdisk_id: VDiskId, key: BobKey, data: BobData) -> Put {
        debug!("PUT[alien][{}] to pearl backend", key);

        let alien_dir = self.alien_dir.clone();
        Put({
            async move {
                let result = alien_dir
                    .write(key, Box::new(data))
                    .map(|r| {
                        r.map(|_ok| BackendPutResult {}).map_err(|e| {
                            debug!(
                                "PUT[alien][{}], error: {:?}",
                                key, e
                            );
                            e
                        })
                    })
                    .await;
                if result.as_ref().err() ==  Some(&backend::Error::VDiskIsNotReady)
                    && alien_dir.try_reinit().await.unwrap() {
                    //TODO panic if failed lock?
                    let _ = alien_dir.reinit_storage().await;
                }
                result
            }
                .boxed()
        })
    }

    fn get(&self, disk_name: String, vdisk_id: VDiskId, key: BobKey) -> Get {
        debug!(
            "Get[{}][{}][{}] from pearl backend",
            disk_name, vdisk_id, key
        );

        let vdisks = self.vdisks.clone();
        Get({
            let vdisk = vdisks.iter().find(|vd| vd.equal(&disk_name, &vdisk_id));
            if let Some(disk) = vdisk {
                let d_clone = disk.clone(); // TODO remove copy of disk. add Box?
                async move {
                    let result = d_clone
                        .read(key)
                        .map(|r| {
                            r.map(|data| BackendGetResult { data }).map_err(|e| {
                                debug!(
                                    "GET[{}][{}][{}], error: {:?}",
                                    disk_name, vdisk_id, key, e
                                );
                                e
                            })
                        })
                        .await;
                    if result.is_err() && d_clone.try_reinit().await.unwrap() {
                        //TODO panic if failed lock?
                        let _ = d_clone.reinit_storage().await;
                    }
                    result
                }
                    .boxed()
            }
            else {
                debug!(
                    "GET[{}][{}][{}] to pearl backend. Cannot find storage",
                    disk_name, vdisk_id, key
                );
                err03(backend::Error::VDiskNoFound(vdisk_id)).boxed()
            }
        })
    }

    fn get_alien(&self, _vdisk_id: VDiskId, key: BobKey) -> Get {
        debug!("Get[alien][{}] from pearl backend", key);

        let alien_dir = self.alien_dir.clone();
        Get({
            async move {
                let result = alien_dir
                    .read(key)
                    .map(|r| {
                        r.map(|data| BackendGetResult { data }).map_err(|e| {
                            debug!(
                                "PUT[alien][{}], error: {:?}",
                                key, e
                            );
                            e
                        })
                    })
                    .await;
                if result.as_ref().err() ==  Some(&backend::Error::VDiskIsNotReady)
                    && alien_dir.try_reinit().await.unwrap() {
                    //TODO panic if failed lock?
                    let _ = alien_dir.reinit_storage().await;
                }
                result
            }
                .boxed()
        })
    }
}

const ALIEN_VDISKID: u32 = 1500512323; //TODO

#[derive(Clone)]
struct PearlVDisk<TSpawner> {
    path: String,
    name: String,
    vdisk: VDiskId,
    disk_path: PathBuf,

    config: PearlConfig,
    spawner: TSpawner,

    storage: Arc<LockGuard<PearlSync>>,
}

impl<TSpawner: Spawn + Clone + Send + 'static + Unpin + Sync> PearlVDisk<TSpawner> {
    pub fn new(
        path: &str,
        name: &str,
        vdisk: VDiskId,
        disk_path: PathBuf,
        config: PearlConfig,
        spawner: TSpawner,
    ) -> Self {
        PearlVDisk {
            path: path.to_string(),
            name: name.to_string(),
            disk_path,
            vdisk,
            config,
            spawner,
            storage: Arc::new(LockGuard::new(PearlSync::new())),
        }
    }
    pub fn new_alien(
        path: &str,
        name: &str,
        disk_path: PathBuf,
        config: PearlConfig,
        spawner: TSpawner,
    ) -> Self {
        PearlVDisk {
            path: path.to_string(),
            name: name.to_string(),
            vdisk: VDiskId::new(ALIEN_VDISKID),
            disk_path,
            config,
            spawner,
            storage: Arc::new(LockGuard::new(PearlSync::new())),
        }
    }

    pub async fn update(&self, storage: Storage<PearlKey>) -> BackendResult<()> {
        trace!("try update Pearl id: {}", self.vdisk);

        let _ = self.storage
            .write_sync_mut(|st| {
                st.set(storage.clone());
                st.ready(); // current pearl disk is ready

                debug!("update Pearl id: {}, mark as ready, state: {}", self.vdisk, st);

                return true;
            })
            .await;

        async move {
            Ok(())
        }.await
    }

    pub fn equal(&self, name: &str, vdisk: &VDiskId) -> bool {
        return self.name == name && self.vdisk == *vdisk;
    }

    pub async fn write(&self, key: BobKey, data: Box<BobData>) -> Result<(), backend::Error> {
        self.storage
            .read(|st| {
                if !st.is_ready() {
                    trace!("Vdisk: {} is not ready for writing, state: {}", self.vdisk, st);
                    return err03(backend::Error::VDiskIsNotReady).boxed(); // TODO mark that typical error
                }
                let storage = st.get();
                trace!("Vdisk: {}, write key: {}", self.vdisk, key);
                Self::write_disk(storage, PearlKey::new(key), data.clone()).boxed()
            })
            .await
    }

    async fn write_disk(
        storage: PearlStorage,
        key: PearlKey,
        data: Box<BobData>,
    ) -> Result<(), backend::Error> {
        storage
            .write(key, PearlData::new(data).bytes())
            .await
            .map_err(|e| {
                trace!("error on write: {:?}", e);
                //TODO check duplicate
                backend::Error::StorageError(format!("{:?}", e))
            })
    }

    pub async fn read(&self, key: BobKey) -> Result<BobData, backend::Error> {
        self.storage
            .read(|st| {
                if !st.is_ready() {
                    trace!("Vdisk: {} is not ready for reading, state: {}", self.vdisk, st);
                    return err03(backend::Error::VDiskIsNotReady).boxed();
                }
                let storage = st.get();
                trace!("Vdisk: {}, read key: {}", self.vdisk, key);
                Self::read_disk(storage, PearlKey::new(key)).boxed()
            })
            .await
    }

    async fn read_disk(storage: PearlStorage, key: PearlKey) -> Result<BobData, backend::Error> {
        storage
            .read(key)
            .await
            .map(|r| PearlData::parse(r))
            .map_err(|e| {
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
                    trace!("Vdisk: {} is currently reinitializing, state: {}", self.vdisk, st);
                    return err03("false".to_string()).boxed();
                }
                st.init();
                trace!("Vdisk: {} set as reinit, state: {}", self.vdisk, st);
                let storage = st.get();
                trace!("Vdisk: {} close old Pearl", self.vdisk);
                let q = async move {
                    let result = storage.close().await;
                    if let Err(e) = result {
                        error!("can't close pearl storage: {:?}", e);
                        return Err(format!("can't close pearl storage: {:?}", e));
                    }
                    Ok(true)
                };
                
                q.boxed()
            })
            .await
    }

    async fn reinit_storage(self) -> BackendResult<()> {
        debug!("Vdisk: {} try reinit Pearl", self.vdisk);
        let mut spawner = self.spawner.clone();
        let _ = async move {
            debug!("Vdisk: {} start reinit Pearl", self.vdisk);
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

    async fn prepare_storage(self) -> BackendResult<()> {
        let repeat = true;
        let path = &self.disk_path;
        let config = self.config.clone();
        let spawner = self.spawner.clone();

        while repeat {
            if let Err(e) = Stuff::check_or_create_directory(path) {
                error!("cannot check path: {:?}, error: {}", path, e);
                //TODO sleep. use config params
                continue;
            }
            
            if let Err(e) = Stuff::drop_pearl_lock_file(path) {
                error!("cannot delete lock file: {:?}, error: {}", path, e);
                //TODO sleep. use config params
                continue;
            }

            let storage = Self::init_pearl_by_path(path, &config); // TODO
            if let Err(e) = storage {
                error!("cannot build pearl by path: {:?}, error: {:?}", path, e);
                //TODO sleep. use config params
                continue;
            }
            let mut st = storage.unwrap();
            if let Err(e) = st.init(spawner.clone()).await {
                error!("cannot init pearl by path: {:?}, error: {:?}", path, e);
                //TODO sleep. use config params
                continue;
            }
            if let Err(e) = self.update(st).await {
                error!("cannot update storage by path: {:?}, error: {:?}", path, e);
                //TODO drop storage
                //TODO sleep. use config params
                continue;
            }
            debug!("Vdisk: {} Pearl is ready for work", self.vdisk);
            return Ok(());
        }
        Err("stub".to_string())
    }

    fn init_pearl_by_path(path: &PathBuf, config: &PearlConfig) -> BackendResult<PearlStorage> {
        let repeat = true;
        while repeat {
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
                .map_err(|e| format!("pearl build error: {:?}", e));
            if let Err(e) = storage {
                error!("cannot build pearl by path: {:?}, error: {}", path, e);
                //TODO sleep. use config params
                continue;
            }
            trace!("Pearl is created by path: {:?}", path);
            return storage;
        }
        Err("stub".to_string())
    }
}

#[derive(Clone, PartialEq, Debug)]
enum PearlState {
    Normal,
    Initializing,
}

#[derive(Clone)]
struct PearlSync {
    storage: Option<PearlStorage>,
    state: PearlState,
}
impl PearlSync {
    pub(crate) fn new() -> Self {
        PearlSync {
            storage: None,
            state: PearlState::Initializing,
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