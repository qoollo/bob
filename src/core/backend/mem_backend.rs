use crate::core::backend::*;
use crate::core::data::{BobData, BobKey};
use futures::future::{err, ok};
use futures_locks::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

struct VDisk {
    repo: HashMap<BobKey, BobData>,
}
 
impl  VDisk {
    pub fn new() -> VDisk {
        VDisk{
            repo: HashMap::<BobKey, BobData>::new()
        }
    }

    pub fn put(&mut self, key: BobKey, data: BobData) {
        self.repo.insert(key, data);
    }

    pub fn get(&self, key: BobKey) -> Option<BobData> {
        match self.repo.get(&key) {
            Some(data) => Some(data.clone()),
            None => None,
        }
    }
}

struct MemDisk {
    name: String,
    vdisks: HashMap<u32, VDisk>,
}

impl MemDisk {
    pub fn new(name: String, vdisks_count: u32) -> MemDisk {
        let mut b: HashMap<u32, VDisk> = HashMap::new();
        for i in 0..vdisks_count {
            b.insert(i, VDisk::new());
        }
        MemDisk {
            name: name.clone(),
            vdisks: b,
        }
    }

    pub fn put(&mut self, vdisk_id: u32, key: BobKey, data: BobData) {
        match self.vdisks.get_mut(&vdisk_id) {
            Some(vdisk) => vdisk.put(key, data),
            None => (), // TODO log
        }
    }

    pub fn get(&self, vdisk_id: u32, key: BobKey) -> Option<BobData> {
        match self.vdisks.get(&vdisk_id) {
            Some(vdisk) => vdisk.get(key),
            None => None, // TODO log
        }
    }
}

#[derive(Clone)]
pub struct MemBackend {
    disks: Arc<RwLock<HashMap<String, MemDisk>>>,
}

impl MemBackend {
    pub fn new(paths: &[String], vdisks_count: u32) -> MemBackend {
        let b = paths.iter()
            .map(|p|(p.clone(), MemDisk::new(p.clone(), vdisks_count)))
            .collect::<HashMap<String, MemDisk>>();
        MemBackend {
            disks: Arc::new(RwLock::new(b)),
        }
    }
}

impl Backend for MemBackend {
    fn put(&self, disk: &String, vdisk_id: u32, key: BobKey, data: BobData) -> BackendPutFuture {
        let disk = disk.clone();
        debug!("PUT[{}][{}]", key, disk);
        Box::new(self
            .disks
            .write()
            .then(move |disks_lock_res| match disks_lock_res {
                Ok(mut disks) => match disks.get_mut(&disk) {
                    Some(mem_disk) => {
                        mem_disk.put(vdisk_id, key, data);
                        ok(BackendResult {})
                    }
                    None => {
                        error!("PUT[{}][{}] Can't find disk {}", key, disk, disk);
                        err(BackendError::NotFound)
                    }
                },
                Err(_) => err(BackendError::Other),
            }))
    }
    fn get(&self, disk: &String, vdisk_id: u32, key: BobKey) -> BackendGetFuture {
        let disk = disk.clone();
        debug!("GET[{}][{}]", key, disk);
        Box::new(self
            .disks
            .read()
            .then(move |disks_lock_res| match disks_lock_res {
                Ok(disks) => match disks.get(&disk) {
                    Some(mem_disk) => match mem_disk.get(vdisk_id, key) {
                                Some(data) => ok(BackendGetResult { data }),
                                None => err(BackendError::NotFound),
                            }
                    None => {
                        error!("GET[{}][{}] Can't find disk {}", key, disk, disk);
                        err(BackendError::NotFound)
                    }
                },
                Err(_) => err(BackendError::Other),
            }))
    }
}
