use crate::core::backend::backend;
use crate::core::backend::backend::*;
use crate::core::backend::pearl::data::*;
use crate::core::backend::pearl::stuff::LockGuard;
use crate::core::configs::node::{NodeConfig, PearlConfig};
use crate::core::data::{BobData, BobKey, VDiskId, VDiskMapper};
use pearl::{Builder, Storage};

use futures03::{future::err as err03, future::ready, task::Spawn, FutureExt, TryFutureExt};

use std::{cell::RefCell, fs::create_dir_all, path::PathBuf, sync::Arc};

pub struct PearlBackend {
    config: PearlConfig,
    vdisks: Arc<Vec<Arc<PearlVDisk>>>,
    alien_dir: Arc<PearlVDisk>,
}

impl PearlBackend {
    pub fn new(mapper: VDiskMapper, config: &NodeConfig) -> Self {
        let pearl_config = config.pearl.clone().unwrap();

        let mut result = Vec::new();

        //init pearl storages for each vdisk
        for disk in mapper.local_disks().iter() {
            let base_path = PathBuf::from(format!("{}/bob/", disk.path));
            while let Err(e) = Self::check_or_create_directory(&base_path) {
                error!(
                    "disk: {}, cannot check path: {:?}, error: {}",
                    disk, base_path, e
                );
                //TODO sleep. use config params. Part 2: make it async? Part 3 : and in separated thread
            }
            let mut vdisks: Vec<Arc<PearlVDisk>> = mapper
                .get_vdisks_by_disk(&disk.name)
                .iter()
                .map(|vdisk_id| {
                    let mut vdisk_path = base_path.clone();
                    vdisk_path.push(format!("{}/", vdisk_id));

                    Arc::new(PearlVDisk::new(
                        &disk.path,
                        &disk.name,
                        vdisk_id.clone(),
                        vdisk_path,
                    ))
                })
                .collect();
            //TODO run vdisk in separate thread?
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
        let alien_dir = PearlVDisk::new_alien(&path, &pearl_config.alien_disk(), alien_path);

        PearlBackend {
            config: pearl_config,
            vdisks: Arc::new(result),
            alien_dir: Arc::new(alien_dir),
        }
    }

    pub async fn run_backend<S>(&self, spawner: S) -> BackendResult<()>
    where
        S: Spawn + Clone + Send + 'static + Unpin + Sync,
    {
        for i in 0..self.vdisks.len() {
            let _ = self
                .prepare_storage(self.vdisks[i].clone(), spawner.clone()) //TODO add Box?
                .await;
        }

        let _ = self
            .prepare_storage(self.alien_dir.clone(), spawner.clone())
            .await;

        Ok(())
    }

    async fn prepare_storage<S>(
        &self,
        pearl_vdisk: Arc<PearlVDisk>,
        spawner: S,
    ) -> BackendResult<()>
    where
        S: Spawn + Clone + Send + 'static + Unpin + Sync,
    {
        let repeat = true;
        let path = &pearl_vdisk.disk_path;
        let config = self.config.clone();
        while repeat {
            if let Err(e) = PearlBackend::check_or_create_directory(path) {
                error!("cannot check path: {:?}, error: {}", path, e);
                //TODO sleep. use config params
                continue;
            }

            let storage = PearlBackend::init_pearl_by_path(path, &config); // TODO
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
            if let Err(e) = pearl_vdisk.update(st).await {
                error!("cannot update storage by path: {:?}, error: {:?}", path, e);
                //TODO drop storage
                //TODO sleep. use config params
                continue;
            }
            return Ok(());
        }
        Err("stub".to_string())
    }

    fn check_or_create_directory(path: &PathBuf) -> BackendResult<()> {
        if !path.exists() {
            return match path.to_str() {
                Some(dir) => create_dir_all(&path)
                    .map(|_r| {
                        info!("create directory: {}", dir);
                        ()
                    })
                    .map_err(|e| {
                        format!("cannot create directory: {}, error: {}", dir, e.to_string())
                    }),
                _ => Err("invalid some path, check vdisk or disk names".to_string()),
            };
        }
        Ok(())
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
                .map_err(|e| format!("Pearl build error: {:?}", e));
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

impl BackendStorage for PearlBackend {
    fn put(&self, disk_name: String, vdisk_id: VDiskId, key: BobKey, data: BobData) -> Put {
        debug!("PUT[{}][{}][{}] to pearl backend", disk_name, vdisk_id, key);

        let vdisks = self.vdisks.clone();
        Put({
            let vdisk = vdisks.iter().find(|vd| vd.equal(&disk_name, &vdisk_id));
            match vdisk {
                Some(disk) => {
                    let d_clone = disk.as_ref().clone(); // TODO remove copy of disk. add Box?
                    async move {
                        d_clone
                            .write(PearlKey::new(key), Box::new(data))
                            .map(|r| {
                                r.map(|_ok| BackendPutResult {}).map_err(|e| {
                                    backend::Error::StorageError(format!(
                                        "PUT[{}][{}][{}], error: {}",
                                        disk_name, vdisk_id, key, e
                                    ))
                                })
                            })
                            .await
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

        let alien_dir = self.alien_dir.as_ref().clone();
        Put({
            async move {
                alien_dir
                .write(PearlKey::new(key), Box::new(data))
                .map(|r| {
                    r.map(|_ok| BackendPutResult {}).map_err(|e| {
                        backend::Error::StorageError(format!(
                            "PUT[alien][{}], error: {}", key, e
                        ))
                    })
                })
                .await
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
                    let d_clone = disk.as_ref().clone(); // TODO remove copy of disk. add Box?
                    async move {
                        d_clone
                            .read(PearlKey::new(key))
                            .map(|r| {
                                r.map(|data| BackendGetResult { data }).map_err(|e| {
                                    backend::Error::StorageError(format!(
                                        "GET[{}][{}][{}], error: {}",
                                        disk_name, vdisk_id, key, e
                                    ))
                                })
                            })
                            .await
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
        
        let alien_dir = self.alien_dir.as_ref().clone();
        Get({
            async move {
                alien_dir
                .read(PearlKey::new(key))
                .map(|r| {
                    r.map(|data| BackendGetResult { data }).map_err(|e| {
                        backend::Error::StorageError(format!(
                            "PUT[alien][{}], error: {}", key, e
                        ))
                    })
                })
                .await
            }
            .boxed()
        })
    }
}

const ALIEN_VDISKID: u32 = 1500512323; //TODO

#[derive(Clone)]
pub(crate) struct PearlVDisk {
    pub path: String,
    pub name: String,
    pub vdisk: VDiskId,
    pub disk_path: PathBuf,
    storage: Arc<LockGuard<PearlSync>>,
}

impl PearlVDisk {
    pub fn new(path: &str, name: &str, vdisk: VDiskId, disk_path: PathBuf) -> Self {
        PearlVDisk {
            path: path.to_string(),
            name: name.to_string(),
            disk_path,
            vdisk,
            storage: Arc::new(LockGuard::new(PearlSync::new())),
        }
    }
    pub fn new_alien(path: &str, name: &str, disk_path: PathBuf) -> Self {
        PearlVDisk {
            path: path.to_string(),
            name: name.to_string(),
            vdisk: VDiskId::new(ALIEN_VDISKID),
            disk_path,
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

    pub async fn write(&self, key: PearlKey, data: Box<BobData>) -> BackendResult<()> {
        self.storage
            .read(|st| {
                if !st.is_ready()
                {
                    return err03("".to_string()).boxed() // TODO mark that typical error
                }
                let storage = st.get();
                Self::write_disk(storage, key.clone(), data.clone()).boxed()
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
            .map_err(|e| format!("error on read: {:?}", e))
    }

    pub async fn read(&self, key: PearlKey) -> BackendResult<BobData> {
        self.storage
            .read::<_, _, Result<String, ()>>(|st| {
                if !st.is_ready()
                {
                    return err03(Err(())).boxed();
                }
                let storage = st.get();
                Self::read_disk(storage, key.clone())
                    .map_err(|e| Ok(e))
                    .boxed()
            })
            .await
            .map_err(|e|
            {
                match e {
                    Ok(error) => {
                        // TODO start reconnect
                        error
                    },
                    _=> "vdisk is not ready".to_string(),
                }
            })
    }

    async fn read_disk(storage: PearlStorage, key: PearlKey) -> BackendResult<BobData> {
        storage
            .read(key)
            .await
            .map(|r| PearlData::parse(r))
            .map_err(|e| format!("error on write: {:?}", e))
            ?
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
    pub(crate) fn is_ready(&self) -> bool {
        self.get_state() == PearlState::Normal
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
