use crate::core::backend::backend;
use crate::core::backend::backend::*;
use crate::core::backend::pearl::{data::*, stuff::{LockGuard, Stuff}};
use crate::core::configs::node::{NodeConfig, PearlConfig};
use crate::core::data::{BobData, BobKey, VDiskId, VDiskMapper};
use pearl::{Builder, Storage};

use futures03::{
    future::err as err03,
    future::ready,
    task::{Spawn, SpawnExt},
    FutureExt,
};

use std::{cell::RefCell, path::PathBuf, sync::Arc};

pub struct PearlBackend<TSpawner> {
    vdisks: Arc<Vec<PearlVDisk<TSpawner>>>,
    alien_dir: PearlVDisk<TSpawner>,
}

impl<TSpawner: Spawn + Clone + Send + 'static + Unpin + Sync> PearlBackend<TSpawner> {
    pub fn new(mapper: VDiskMapper, config: &NodeConfig, spawner: TSpawner) -> Self {
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

    pub async fn run_backend(&self) -> BackendResult<()> {
        for i in 0..self.vdisks.len() {
            let _ = self.vdisks[i]
                .clone()
                .prepare_storage() //TODO add Box?
                .await;
        }

        let _ = self.alien_dir.clone().prepare_storage().await;

        Ok(())
    }
}

impl<TSpawner: Spawn + Clone + Send + 'static + Unpin + Sync> BackendStorage
    for PearlBackend<TSpawner>
{
    fn put(&self, disk_name: String, vdisk_id: VDiskId, key: BobKey, data: BobData) -> Put {
        debug!("PUT[{}][{}][{}] to pearl backend", disk_name, vdisk_id, key);

        let vdisks = self.vdisks.clone();
        Put({
            let vdisk = vdisks.iter().find(|vd| vd.equal(&disk_name, &vdisk_id));
            match vdisk {
                Some(disk) => {
                    let d_clone = disk.clone(); // TODO remove copy of disk. add Box?
                    async move {
                        let result = d_clone
                            .write(key, Box::new(data))
                            .map(|r| {
                                r.map(|_ok| BackendPutResult {}).map_err(|e| {
                                    backend::Error::StorageError(format!(
                                        "PUT[{}][{}][{}], error: {}",
                                        disk_name, vdisk_id, key, e
                                    ))
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
                _ => {
                    debug!(
                        "PUT[{}][{}][{}] to pearl backend. Cannot find storage",
                        disk_name, vdisk_id, key
                    );
                    err03(backend::Error::VDiskNoFound(vdisk_id)).boxed()
                }
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
                            backend::Error::StorageError(format!(
                                "PUT[alien][{}], error: {}",
                                key, e
                            ))
                        })
                    })
                    .await;
                if result.is_err() && alien_dir.try_reinit().await.unwrap() {
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
            match vdisk {
                Some(disk) => {
                    let d_clone = disk.clone(); // TODO remove copy of disk. add Box?
                    async move {
                        let result = d_clone
                            .read(PearlKey::new(key))
                            .map(|r| {
                                r.map(|data| BackendGetResult { data }).map_err(|e| {
                                    backend::Error::StorageError(format!(
                                        "GET[{}][{}][{}], error: {}",
                                        disk_name, vdisk_id, key, e
                                    ))
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
                _ => {
                    debug!(
                        "GET[{}][{}][{}] to pearl backend. Cannot find storage",
                        disk_name, vdisk_id, key
                    );
                    err03(backend::Error::VDiskNoFound(vdisk_id)).boxed()
                }
            }
        })
    }

    fn get_alien(&self, _vdisk_id: VDiskId, key: BobKey) -> Get {
        debug!("Get[alien][{}] from pearl backend", key);

        let alien_dir = self.alien_dir.clone();
        Get({
            async move {
                let result = alien_dir
                    .read(PearlKey::new(key))
                    .map(|r| {
                        r.map(|data| BackendGetResult { data }).map_err(|e| {
                            backend::Error::StorageError(format!(
                                "PUT[alien][{}], error: {}",
                                key, e
                            ))
                        })
                    })
                    .await;
                if result.is_err() && alien_dir.try_reinit().await.unwrap() {
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
        self.storage
            .write(|st| {
                st.set(storage.clone());
                st.ready(); // current pearl disk is ready
                ready(Ok(())).boxed()
            })
            .await
    }

    pub fn equal(&self, name: &str, vdisk: &VDiskId) -> bool {
        return self.name == name && self.vdisk == *vdisk;
    }

    pub async fn write(&self, key: BobKey, data: Box<BobData>) -> BackendResult<()> {
        self.storage
            .read(|st| {
                if !st.is_ready() {
                    return err03("".to_string()).boxed(); // TODO mark that typical error
                }
                let storage = st.get();
                Self::write_disk(storage, PearlKey::new(key), data.clone()).boxed()
            })
            .await
    }

    async fn write_disk(
        storage: PearlStorage,
        key: PearlKey,
        data: Box<BobData>,
    ) -> BackendResult<()> {
        storage
            .write(key, PearlData::new(data).bytes())
            .await
            .map_err(|e| format!("error on write: {:?}", e))
    }

    pub async fn read(&self, key: PearlKey) -> BackendResult<BobData> {
        self.storage
            .read(|st| {
                if !st.is_ready() {
                    return err03("vdisk is not ready".to_string()).boxed();
                }
                let storage = st.get();
                Self::read_disk(storage, key.clone()).boxed()
            })
            .await
    }

    async fn read_disk(storage: PearlStorage, key: PearlKey) -> BackendResult<BobData> {
        storage
            .read(key)
            .await
            .map(|r| PearlData::parse(r))
            .map_err(|e| format!("error on read: {:?}", e))?
    }

    pub async fn try_reinit(&self) -> BackendResult<bool> {
        self.storage
            .write_sync(|st| {
                if st.is_reinit() {
                    return false;
                }
                st.init();
                let storage = st.get();
                let result = storage.close();
                if let Err(e) = result {
                    error!("can't close pearl storage: {:?}", e);
                }
                return true;
            })
            .await
    }

    async fn reinit_storage(self) -> BackendResult<()> {
        let mut spawner = self.spawner.clone();
        let _ = async move {
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
            return storage;
        }
        Err("stub".to_string())
    }
}

#[derive(Clone, PartialEq)]
enum PearlState {
    Normal,
    Initializing,
}

#[derive(Clone)]
struct PearlSync {
    storage: RefCell<Option<PearlStorage>>,
    state: RefCell<PearlState>,
}
impl PearlSync {
    pub(crate) fn new() -> Self {
        PearlSync {
            storage: RefCell::new(None),
            state: RefCell::new(PearlState::Initializing),
        }
    }
    pub(crate) fn ready(&self) {
        self.set_state(PearlState::Normal);
    }
    pub(crate) fn init(&self) {
        self.set_state(PearlState::Initializing);
    }
    pub(crate) fn is_ready(&self) -> bool {
        self.get_state() == PearlState::Normal
    }
    pub(crate) fn is_reinit(&self) -> bool {
        self.get_state() == PearlState::Initializing
    }

    pub(crate) fn set_state(&self, state: PearlState) {
        self.state.replace(state);
    }
    pub(crate) fn get_state(&self) -> PearlState {
        self.state.borrow().clone()
    }

    pub(crate) fn set(&self, storage: PearlStorage) {
        self.storage.replace(Some(storage));
    }
    pub(crate) fn get(&self) -> PearlStorage {
        self.storage.borrow().clone().unwrap()
    }
}
