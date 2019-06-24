use crate::core::backend::backend;
use crate::core::backend::backend::*;
use crate::core::configs::node::{NodeConfig, PearlConfig};
use crate::core::data::{BobData, BobKey, BobMeta, VDiskId, VDiskMapper, DiskPath};
use futures_locks::RwLock;
use pearl::{Builder, Key, Storage};

use futures::future::{err, ok, Future};
use futures03::{
    compat::Future01CompatExt,
    executor::{ThreadPool, ThreadPoolBuilder},
    future::err as err03,
    task::Spawn,
    Future as Future03, FutureExt, TryFutureExt,
};

use std::{
    convert::TryInto,
    fs::create_dir_all,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
};

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

    pub async fn init2<S>(&mut self, mapper: VDiskMapper, mut _spawner: S) -> Result<(), String>
    where
        S: Spawn + Clone + Send + 'static + Unpin + Sync,
    {
        let mut result = Vec::new();

        //init pearl storages for each vdisk
        for disk in mapper.local_disks().iter() {
            let base_path = PathBuf::from(format!("{}/bob/", disk.path));
            Self::check_or_create_directory2(&base_path).unwrap(); //TODO handle fail and try restart. Part2: we should panic because base path is invalid?

            let mut vdisks: Vec<PearlVDisk> = mapper
                .get_vdisks_by_disk(&disk.name)
                .iter()
                .map(|vdisk_id| {
                    let mut vdisk_path = base_path.clone();
                    vdisk_path.push(format!("{}/", vdisk_id.clone()));
                    Self::check_or_create_directory(&vdisk_path).unwrap(); //TODO handle fail and try restart

                    let mut storage = Self::init_pearl_by_path(vdisk_path, &self.config);
                    self.run_storage(&mut storage);

                    PearlVDisk::new(&disk.path, &disk.name, vdisk_id.clone(), storage)
                })
                .collect();
            result.append(&mut vdisks);
        }
        self.vdisks = Arc::new(result);

        unimplemented!();
    }
#[allow(dead_code)]
    async fn init_disk<S>(&self, disk: DiskPath, pearl_vdisks: Arc<Vec<Arc<PearlVDiskGuard>>>, spawner: S) -> Result<(), ()>
    where
        S: Spawn + Clone + Send + 'static + Unpin + Sync,
    {
        let base_path = PathBuf::from(format!("{}/bob/", disk.path));
        while let Err(e) = Self::check_or_create_directory2(&base_path){
            error!("disk: {}, cannot check path: {:?}, error: {}", disk, base_path, e);
            //TODO sleep. use config params
        }
        
        for i in 0..pearl_vdisks.len() {
            let mut vdisk_path = base_path.clone();
            let vdisk_id = pearl_vdisks[i].get(|vd| vd.vdisk.clone()).await?; //TODO
            vdisk_path.push(format!("{}/", vdisk_id));

            let storage = Self::prepare_storage(vdisk_path, spawner.clone(), self.config.clone()).await;
            match storage {
                Ok(st) =>  {
                    pearl_vdisks[i].update(st).await; // TODO log
                },
                _ => {}, //TODO log
            }
        }
        unimplemented!();
    }

    pub async fn prepare_storage<S>(path: PathBuf, spawner: S, config: PearlConfig) -> Result<PearlStorage, ()>
    where
        S: Spawn + Clone + Send + 'static + Unpin + Sync,
    {
        let repeat = true;
        while repeat {
            if let Err(e) = PearlBackend::check_or_create_directory2(&path){
                error!("cannot check path: {:?}, error: {}", path, e);
                //TODO sleep. use config params
                continue;
            }

            let storage = PearlBackend::init_pearl_by_path2(path.clone(), &config.clone());
            match storage {
                Err(e) => {
                    error!("cannot build pearl by path: {:?}, error: {}", path, e);
                    //TODO sleep. use config params
                    continue;
                }
                Ok(mut st) => {
                    if let Err(e) = st.init(spawner.clone()).await {
                        error!("cannot init pearl by path: {:?}, error: {:?}", path, e);
                        //TODO sleep. use config params
                        continue;
                    }
                    return Ok(st)
                }
            }
        }
        Err(())
    }

    fn check_or_create_directory2(path: &Path) -> Result<(), String> {
        if !path.exists() {
            return match path.to_str() {
                Some(dir) =>  create_dir_all(path)
                                .map(|_r| {
                                    info!("created directory: {}", dir);
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
    fn init_pearl_by_path2(path: PathBuf, config: &PearlConfig) -> Result<PearlStorage, String> {
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

        builder.build()
            .map_err(|e| format!("Pearl build error: {:?}", e))
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

                    let mut storage = Self::init_pearl_by_path(vdisk_path, &self.config);
                    self.run_storage(&mut storage);

                    PearlVDisk::new(&disk.path, &disk.name, vdisk_id.clone(), storage)
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
        let mut storage = Self::init_pearl_by_path(alien_path, &self.config);
        self.run_storage(&mut storage);

        self.alien_dir = Arc::new(Some(PearlVDisk::new_alien(
            &path,
            &self.config.alien_disk(),
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
                    .map_err(|_e: ()| backend::Error::StorageError)
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
                    .map_err(|_e: ()| backend::Error::StorageError)
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
                            .map_err(|_e: ()| backend::Error::StorageError)
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
                            .map_err(|_e: ()| backend::Error::StorageError)
                    })
                    .boxed() //TODO - add description for error
            } else {
                debug!("Get[alien][{}] to pearl backend. Cannot find storage", key);
                err03(backend::Error::VDiskNoFound(vdisk_id)).boxed()
            }
        })
    }
}

#[allow(dead_code)]
struct PearlVDiskGuard {
    storage: Arc<RwLock<PearlVDisk>>,
}

#[allow(dead_code)]
impl PearlVDiskGuard {
    #[allow(dead_code)]
    pub async fn read<F, Ret>(&self, f:F) -> Result<Ret, ()>
        where F: Fn(PearlStorage) -> Pin<Box<dyn Future03<Output = Result<Ret, ()>>+Send>> + Send+Sync,
     {
        self.storage
            .read()
            .map(move |st| {
                let storage = st.storage.clone();
                f(storage)
                    .map_err(|_e| ())
            })
            .map_err(|_e| {}) //TODO log
            .compat()
            .boxed()
            .await
            .unwrap()
            .await
    }

    #[allow(dead_code)]
    pub async fn write<F, Ret>(&self, f:F) -> Result<Ret, ()>
        where F: Fn(PearlStorage) -> Pin<Box<dyn Future03<Output = Result<Ret, ()>>+Send>> + Send+Sync,
     {
        self.storage
            .write()
            .map(move |st| {
                let storage = st.storage.clone();
                f(storage)
                    .map_err(|_e| ())
            })
            .map_err(|_e| {}) //TODO log
            .compat()
            .boxed()
            .await
            .unwrap()
            .await
    }

    pub async fn update(&self, pearl: PearlStorage) -> Result<(), ()>
     {
        let mut storage = self.storage
            .write().compat().await
            .map_err(|_e| {}).unwrap(); //TODO log

        storage.storage = pearl;
        Ok(())
    }

    pub async fn get<F, Ret>(&self, f:F) -> Result<Ret, ()>
        where F: Fn(&PearlVDisk) -> Ret + Send+Sync,
     {
        self.storage
            .read()
            .map(move |st| {
                f(&*st)
            })
            .map_err(|_e| {}) //TODO log
            .compat()
            .boxed()
            .await
    }
}

const ALIEN_VDISKID: u32 = 1500512323; //TODO
#[derive(Clone)]
struct PearlVDisk {
    pub path: String,
    pub name: String,
    pub vdisk: VDiskId,
    pub storage: PearlStorage,
}

impl PearlVDisk {
    pub fn new(path: &str, name: &str, vdisk: VDiskId, storage: Storage<PearlKey>) -> Self {
        PearlVDisk {
            path: path.to_string(),
            name: name.to_string(),
            vdisk,
            storage,
        }
    }
    pub fn new_alien(path: &str, name: &str, storage: Storage<PearlKey>) -> Self {
        PearlVDisk {
            path: path.to_string(),
            name: name.to_string(),
            vdisk: VDiskId::new(ALIEN_VDISKID),
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

#[allow(dead_code)]
    pub async fn init_vdisk<'a, S>(&'a mut self, path: PathBuf, spawner: S, config: Arc<PearlConfig>) -> Result<(), ()>
    where
        S: Spawn + Clone + Send + 'static + Unpin + Sync,
    {
        let mut repeat = true;
        while repeat {
            if let Err(e) = PearlBackend::check_or_create_directory2(&path){
                error!("cannot check path: {:?}, error: {}", path, e);
                //TODO sleep. use config params
                continue;
            }

            let storage = PearlBackend::init_pearl_by_path2(path.clone(), &config.clone());
            match storage {
                Err(e) => {
                    error!("cannot build pearl by path: {:?}, error: {}", path, e);
                    //TODO sleep. use config params
                    continue;
                }
                Ok(mut st) => {
                    if let Err(e) = st.init(spawner.clone()).await {
                        error!("cannot init pearl by path: {:?}, error: {:?}", path, e);
                        //TODO sleep. use config params
                        continue;
                    }
                    //TODO set storage to pearl
                    self.storage = st;
                    repeat = false;
                }
            }
        }
        Ok(())
    }
}

#[allow(dead_code)]
enum State {
    Ready,
    Initializing,
}

impl State {
    #[allow(dead_code)]
    fn is_ready(&self) -> bool {
        match self {
            State::Ready => true,
            _ => false,
        }
    }
}

#[allow(dead_code)]
struct PearlState {
    state: Arc<RwLock<State>>,
}

impl PearlState {
    #[allow(dead_code)]
    fn new() -> Self {
        PearlState {
            state: Arc::new(RwLock::new(State::Initializing)),
        }
    }

#[allow(dead_code)]
    fn is_ready(&self) -> Pin<Box<dyn Future03<Output = Result<bool, ()>> + Send>> {
        self.state
            .read()
            .then(move |state_lock| match state_lock {
                Ok(state) => ok(state.is_ready()),
                Err(_) => err({}),
            })
            .compat()
            .boxed()
    }
#[allow(dead_code)]
    fn lock(&self) -> Pin<Box<dyn Future03<Output = Result<(), ()>> + Send>> {
        self.state
            .write()
            .map(move |mut state| {
                *state = State::Initializing;
                ()
            })
            .map_err(|_e| {}) //TODO log
            .compat()
            .boxed()
    }
#[allow(dead_code)]
    fn ready(&self) -> Pin<Box<dyn Future03<Output = Result<(), ()>> + Send>> {
        self.state
            .write()
            .map(move |mut state| {
                *state = State::Ready;
                ()
            })
            .map_err(|_e| {})//TODO log
            .compat()
            .boxed()
    }
}
