use crate::core::backend::backend;
use crate::core::backend::backend::*;
use crate::core::configs::node::{NodeConfig, PearlConfig};
use crate::core::data::{BobData, BobKey, BobMeta, VDiskId, VDiskMapper};
use pearl::{Builder, Key, Storage};

use futures03::{
    executor::{ThreadPool, ThreadPoolBuilder},
    future::err as err03,
    FutureExt, TryFutureExt,
};

use std::{
    convert::TryInto,
    fs::create_dir_all,
    path::{Path, PathBuf},
    sync::Arc,
};

pub type BackendResult<T> = Result<T, String>;
pub type PearlStorage = Storage<PearlKey>;

pub struct PearlKey {
    pub key: Vec<u8>,
}
impl PearlKey {
    pub fn new(key: BobKey) -> Self {
        PearlKey {
            key: key.key.clone().to_be_bytes().to_vec(),
        }
    }
}
impl Key for PearlKey {
    const LEN: u16 = 8;
}
impl AsRef<[u8]> for PearlKey {
    fn as_ref(&self) -> &[u8] {
        &self.key
    }
}

struct PearlData {
    data: Vec<u8>,
    timestamp: u32,
}
impl PearlData {
    const TIMESTAMP_LEN: usize = 4;

    pub fn new(data: BobData) -> Self {
        PearlData {
            data: data.data,
            timestamp: data.meta.timestamp,
        }
    }

    pub fn bytes(&mut self) -> Vec<u8> {
        let mut result = self.timestamp.to_be_bytes().to_vec();
        result.append(&mut self.data);
        result
    }

    pub fn parse(data: Vec<u8>) -> BobData {
        let (tmp, bob_data) = data.split_at(PearlData::TIMESTAMP_LEN);
        let timestamp = u32::from_be_bytes(tmp.try_into().unwrap()); //TODO check error
        BobData::new(bob_data.to_vec(), BobMeta::new_value(timestamp))
    }
}

#[allow(dead_code)]
pub struct PearlBackend {
    config: PearlConfig,
    pool: ThreadPool,
    vdisks: Arc<Vec<PearlVDisk>>,
    alien_dir: Arc<Option<PearlVDisk>>,
}

impl PearlBackend {
    pub fn new(config: &NodeConfig) -> Self {
        let pearl_config = config.pearl.clone().unwrap();
        let pool = ThreadPoolBuilder::new()
            .pool_size(pearl_config.pool_count_threads() as usize)
            .create()
            .unwrap();

        PearlBackend {
            config: pearl_config,
            pool,
            vdisks: Arc::new(vec![]),
            alien_dir: Arc::new(None),
        }
    }

    pub fn init(&mut self, mapper: &VDiskMapper) -> Result<(), String> {
        let mut result = Vec::new();

        //init pearl storages for each vdisk
        for disk in mapper.local_disks().iter() {
            let base_path = PathBuf::from(format!("{}/bob/", disk.path));
            Self::check_or_create_directory(&base_path).unwrap(); //TODO handle fail and try restart

            let mut vdisks: Vec<PearlVDisk> = mapper
                .get_vdisks_by_disk(&disk.name)
                .iter()
                .map(|vdisk_id| {
                    let mut vdisk_path = base_path.clone();
                    vdisk_path.push(format!("{}/", vdisk_id.clone()));
                    Self::check_or_create_directory(&vdisk_path).unwrap(); //TODO handle fail and try restart

                    let mut storage = Self::init_pearl_by_path(vdisk_path.clone(), &self.config);
                    self.run_storage(&mut storage);

                    PearlVDisk::new(
                        &disk.path,
                        &disk.name,
                        vdisk_id.clone(),
                        vdisk_path,
                        storage,
                    )
                })
                .collect();
            result.append(&mut vdisks);
        }
        self.vdisks = Arc::new(result);

        //init alien storage
        let path = format!(
            "{}/alien/",
            mapper
                .get_disk_by_name(&self.config.alien_disk())
                .unwrap()
                .path
        );
        let alien_path = PathBuf::from(path.clone());
        Self::check_or_create_directory(&alien_path).unwrap(); //TODO handle fail and try restart
        let mut storage = Self::init_pearl_by_path(alien_path.clone(), &self.config);
        self.run_storage(&mut storage);

        self.alien_dir = Arc::new(Some(PearlVDisk::new_alien(
            &path,
            &self.config.alien_disk(),
            alien_path,
            storage,
        )));

        Ok(())
    }

    fn check_or_create_directory(path: &Path) -> Result<(), String> {
        if !path.exists() {
            let dir = path.to_str().unwrap(); //TODO handle fail and try restart
            return create_dir_all(path)
                .map(|_r| {
                    info!("created directory: {}", dir);
                    ()
                })
                .map_err(|e| {
                    format!("cannot create directory: {}, error: {}", dir, e.to_string())
                });
        }
        Ok(())
    }

    fn init_pearl_by_path(path: PathBuf, config: &PearlConfig) -> PearlStorage {
        let mut builder = Builder::new().work_dir(path);

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

        builder.build().unwrap() //TODO handle fail and try restart
    }

    fn run_storage(&mut self, storage: &mut PearlStorage) {
        // init could take a while
        self.pool.run(storage.init(self.pool.clone())).unwrap(); //TODO handle fail and try restart
    }
}

impl BackendStorage for PearlBackend {
    fn put(&self, disk_name: String, vdisk_id: VDiskId, key: BobKey, data: BobData) -> Put {
        debug!("PUT[{}][{}][{}] to pearl backend", disk_name, vdisk_id, key);

        //TODO remove clone for vdisk_id
        let t = self.vdisks.clone();
        Put({
            let vdisk = t.iter().find(|vd| vd.equal(&disk_name, vdisk_id.clone()));

            if let Some(disk) = vdisk {
                let storage = disk.storage.clone();
                PearlVDisk::write(storage, PearlKey::new(key), data)
                    .map(|_r| Ok(BackendPutResult {}))
                    .map_err(|_e: ()| backend::Error::StorageError("".to_string()))
                    .boxed() //TODO - add description for error key or vdisk for example
            } else {
                debug!(
                    "PUT[{}][{}][{}] to pearl backend. Cannot find storage",
                    disk_name, vdisk_id, key
                );
                err03(backend::Error::VDiskNoFound(vdisk_id)).boxed()
            }
        })
    }

    fn put_alien(&self, vdisk_id: VDiskId, key: BobKey, data: BobData) -> Put {
        debug!("PUT[alien][{}] to pearl backend", key);

        //TODO remove clone for vdisk_id
        let vdisk = self.alien_dir.as_ref().clone();
        Put({
            if let Some(disk) = vdisk {
                let storage = disk.storage.clone();
                PearlVDisk::write(storage, PearlKey::new(key), data)
                    .map(|_r| Ok(BackendPutResult {}))
                    .map_err(|_e: ()| backend::Error::StorageError("".to_string()))
                    .boxed() //TODO - add description for error
            } else {
                debug!("PUT[alien][{}] to pearl backend. Cannot find storage", key);
                err03(backend::Error::VDiskNoFound(vdisk_id)).boxed()
            }
        })
    }

    fn get(&self, disk_name: String, vdisk_id: VDiskId, key: BobKey) -> Get {
        debug!(
            "Get[{}][{}][{}] from pearl backend",
            disk_name, vdisk_id, key
        );

        //TODO remove clone for vdisk_id
        let t = self.vdisks.clone();
        Get({
            let vdisk = t.iter().find(|vd| vd.equal(&disk_name, vdisk_id.clone()));

            if let Some(disk) = vdisk {
                let storage = disk.storage.clone();
                PearlVDisk::read(storage, PearlKey::new(key))
                    .map(|r| {
                        r.map(|data| BackendGetResult { data })
                            .map_err(|_e: ()| backend::Error::StorageError("".to_string()))
                    })
                    .boxed() //TODO - add description for error
            } else {
                debug!(
                    "Get[{}][{}][{}] to pearl backend. Cannot find storage",
                    disk_name, vdisk_id, key
                );
                err03(backend::Error::VDiskNoFound(vdisk_id)).boxed()
            }
        })
    }

    fn get_alien(&self, vdisk_id: VDiskId, key: BobKey) -> Get {
        debug!("Get[alien][{}] from pearl backend", key);
        let vdisk = self.alien_dir.as_ref().clone();
        Get({
            if let Some(disk) = vdisk {
                let storage = disk.storage.clone();
                PearlVDisk::read(storage, PearlKey::new(key))
                    .map(|r| {
                        r.map(|data| BackendGetResult { data })
                            .map_err(|_e: ()| backend::Error::StorageError("".to_string()))
                    })
                    .boxed() //TODO - add description for error
            } else {
                debug!("Get[alien][{}] to pearl backend. Cannot find storage", key);
                err03(backend::Error::VDiskNoFound(vdisk_id)).boxed()
            }
        })
    }
}

const ALIEN_VDISKID: u32 = 1500512323; //TODO
#[derive(Clone)]
struct PearlVDisk {
    pub path: String,
    pub name: String,
    pub vdisk: VDiskId,
    pub disk_path: PathBuf,
    pub storage: PearlStorage,
}

impl PearlVDisk {
    pub fn new(
        path: &str,
        name: &str,
        vdisk: VDiskId,
        disk_path: PathBuf,
        storage: Storage<PearlKey>,
    ) -> Self {
        PearlVDisk {
            path: path.to_string(),
            name: name.to_string(),
            disk_path,
            vdisk,
            storage,
        }
    }
    pub fn new_alien(
        path: &str,
        name: &str,
        disk_path: PathBuf,
        storage: Storage<PearlKey>,
    ) -> Self {
        PearlVDisk {
            path: path.to_string(),
            name: name.to_string(),
            vdisk: VDiskId::new(ALIEN_VDISKID),
            disk_path,
            storage,
        }
    }

    pub fn equal(&self, name: &str, vdisk: VDiskId) -> bool {
        return self.name == name && self.vdisk == vdisk;
    }

    pub async fn write(storage: PearlStorage, key: PearlKey, data: BobData) -> Result<(), ()> {
        storage
            .write(key, PearlData::new(data).bytes())
            .await
            .map_err(|_e| ()) // TODO make error public, check bytes
    }

    pub async fn read(storage: PearlStorage, key: PearlKey) -> Result<BobData, ()> {
        storage
            .read(key)
            .await
            .map(|r| PearlData::parse(r))
            .map_err(|_e| ()) // TODO make error public, check bytes
    }
}
