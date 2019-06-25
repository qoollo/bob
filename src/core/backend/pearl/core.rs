// use crate::core::backend::backend;
// use crate::core::backend::backend::*;
use crate::core::configs::node::{NodeConfig, PearlConfig};
use crate::core::data::{BobData, BobKey, BobMeta, VDiskId, VDiskMapper};
use crate::core::backend::pearl::stuff::LockGuard;
use pearl::{Builder, Key, Storage};
use crate::core::backend::pearl::data::*;

use futures03::{
    task::Spawn,
    FutureExt,
    future::ready,    
};

use std::{
    // convert::TryInto,
    fs::create_dir_all,
    path::PathBuf,
    cell::RefCell,
    sync::Arc,
};

pub struct PearlBackend {
    config: PearlConfig,
    vdisks: Arc<Vec<PearlVDisk>>,
    alien_dir: Arc<Option<PearlVDisk>>,
}

impl PearlBackend {
    pub fn new(config: &NodeConfig) -> Self {
        let pearl_config = config.pearl.clone().unwrap();

        PearlBackend {
            config: pearl_config,
            vdisks: Arc::new(vec![]),
            alien_dir: Arc::new(None),
        }
    }

    pub async fn init<S>(&mut self, mapper: VDiskMapper, _spawner: S) -> BackendResult<()>
    where
        S: Spawn + Clone + Send + 'static + Unpin + Sync,
    {
        let mut result = Vec::new();

        //init pearl storages for each vdisk
        for disk in mapper.local_disks().iter() {
            let base_path = PathBuf::from(format!("{}/bob/", disk.path));
            while let Err(e) = Self::check_or_create_directory(&base_path){
                error!("disk: {}, cannot check path: {:?}, error: {}", disk, base_path, e);
                //TODO sleep. use config params. Part 2: make it async? Part 3 : and in separated thread
            }
            let mut vdisks: Vec<PearlVDisk> = mapper
                .get_vdisks_by_disk(&disk.name)
                .iter()
                .map(|vdisk_id| {
                    let mut vdisk_path = base_path.clone();
                    vdisk_path.push(format!("{}/", vdisk_id));

                    PearlVDisk::new(&disk.path, &disk.name, vdisk_id.clone(), vdisk_path)
                })
                .collect();
                //TODO run vdisk in separate thread?
            result.append(&mut vdisks);
        }
        // self.vdisks = Arc::new(result);

        unimplemented!();
    }

    async fn prepare_storage<S>(pearl_vdisk: &PearlVDisk, spawner: S, config: PearlConfig) -> BackendResult<()>
    where
        S: Spawn + Clone + Send + 'static + Unpin + Sync,
    {
        let repeat = true;
        let path = &pearl_vdisk.disk_path;
        while repeat {
            if let Err(e) = PearlBackend::check_or_create_directory(path){
                error!("cannot check path: {:?}, error: {}", path, e);
                //TODO sleep. use config params
                continue;
            }

            let storage = PearlBackend::init_pearl_by_path(path, &config.clone()); // TODO
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
                Some(dir) =>  create_dir_all(&path)
                                .map(|_r| {
                                    info!("create directory: {}", dir);
                                    ()
                                })
                                .map_err(|e| {
                                    format!("cannot create directory: {}, error: {}", dir, e.to_string())
                                }),
                _ => Err("invalid some path, check vdisk or disk names".to_string()),
                }
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

            let storage = builder.build()
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

const ALIEN_VDISKID: u32 = 1500512323; //TODO

pub(crate) struct PearlVDisk {
    pub path: String,
    pub name: String,
    pub vdisk: VDiskId,
    pub disk_path: PathBuf,
    storage: LockGuard<PearlSync>,
}

impl PearlVDisk {
    pub fn new(path: &str, name: &str, vdisk: VDiskId, disk_path: PathBuf) -> Self {
        PearlVDisk {
            path: path.to_string(),
            name: name.to_string(),
            disk_path,
            vdisk,
            storage: LockGuard::new(PearlSync::new()),
        }
    }
    pub fn new_alien(path: &str, name: &str, disk_path: PathBuf) -> Self {
        PearlVDisk {
            path: path.to_string(),
            name: name.to_string(),
            vdisk: VDiskId::new(ALIEN_VDISKID),
            disk_path,
            storage: LockGuard::new(PearlSync::new()),
        }
    }

    pub async fn update(&self, storage: Storage<PearlKey>) -> BackendResult<()> {
        self.storage.write(|st| {
            st.set(storage.clone());
            ready(Ok(())).boxed()
        }).await
    }

    pub fn equal(&self, name: &str, vdisk: VDiskId) -> bool {
        return self.name == name && self.vdisk == vdisk;
    }
    
    pub async fn write(&self, key: PearlKey, data: Box<BobData>) -> BackendResult<()> {
        self.storage.read(|st| {
            let storage = st.get();
            Self::write_disk(storage, key.clone(), data.clone()).boxed()
        }).await
    }

    async fn write_disk(storage: PearlStorage, key: PearlKey, data: Box<BobData>) -> BackendResult<()> {
        storage
            .write(key, PearlData::new(data).bytes())
            .await
            .map_err(|e| format!("error on read: {:?}", e)) // TODO make error public, check bytes
    }

    pub async fn read(&self, key: PearlKey) -> BackendResult<BobData> {
        self.storage.read(|st| {
            let storage = st.get();
            Self::read_disk(storage, key.clone()).boxed()
        }).await
    }

    async fn read_disk(storage: PearlStorage, key: PearlKey) -> BackendResult<BobData> {
        storage
            .read(key)
            .await
            .map(|r| PearlData::parse(r))
            .map_err(|e| format!("error on write: {:?}", e)) // TODO make error public, check bytes
    }
}

#[derive(Clone)]
struct PearlSync {
    storage: RefCell<Option<PearlStorage>>,
    //TODO add state with arc
}
impl PearlSync {
    pub(crate) fn new()->Self{
        PearlSync{
            storage: RefCell::new(None),
        }
    }

    pub(crate) fn set(&self, storage: PearlStorage){
        self.storage.replace(Some(storage));
    }

    pub(crate) fn get(&self) -> PearlStorage {
        self.storage.borrow().clone().unwrap()
    }
}