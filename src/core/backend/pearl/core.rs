// use crate::core::backend::backend;
// use crate::core::backend::backend::*;
// use crate::core::configs::node::{NodeConfig, PearlConfig};
use crate::core::data::{BobData, BobKey, BobMeta, VDiskId, VDiskMapper};
use crate::core::backend::pearl::stuff::LockGuard;
use pearl::{Builder, Key, Storage};
use crate::core::backend::pearl::data::*;
use futures::future::{err, ok, Future};

use futures03::{
    // compat::Future01CompatExt,
    // executor::{ThreadPool, ThreadPoolBuilder},
    future::err as err03,
    // task::Spawn,
    Future as Future03, FutureExt, TryFutureExt,
    future::ready,    
};

use std::{
    // convert::TryInto,
    // fs::create_dir_all,
    path::{Path, PathBuf},
    // pin::Pin,
    cell::RefCell,

};

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