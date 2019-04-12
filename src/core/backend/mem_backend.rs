use crate::core::backend::*;
use crate::core::data::{BobData, BobKey};
use futures::future::{err, ok};
use futures_locks::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

struct MemDisk {
    repo: HashMap<BobKey, BobData>,
}

impl MemDisk {
    pub fn new() -> MemDisk {
        MemDisk {
            repo: HashMap::new(),
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

#[derive(Clone)]
pub struct MemBackend {
    vdisks: Arc<RwLock<HashMap<String, MemDisk>>>,
}

impl MemBackend {
    pub fn new(paths: &[String]) -> MemBackend {
        let mut b: HashMap<String, MemDisk> = HashMap::new();
        for p in paths.iter() {
            b.insert(p.clone(), MemDisk::new());
        }
        MemBackend {
            vdisks: Arc::new(RwLock::new(b)),
        }
    }
}

impl Backend for MemBackend {
    fn put(&self, disk: &String, key: BobKey, data: BobData) -> BackendPutFuture {
        let disk = disk.clone();
        debug!("PUT[{}][{}]", key, disk);
        Box::new(self
            .vdisks
            .write()
            .then(move |vdisks_lock_res| match vdisks_lock_res {
                Ok(mut vdisks) => match vdisks.get_mut(&disk) {
                    Some(mem_disk) => {
                        mem_disk.put(key, data);
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
    fn get(&self, disk: &String, key: BobKey) -> BackendGetFuture {
        let disk = disk.clone();
        debug!("GET[{}][{}]", key, disk);
        Box::new(self
            .vdisks
            .read()
            .then(move |vdisks_lock_res| match vdisks_lock_res {
                Ok(vdisks) => match vdisks.get(&disk) {
                    Some(mem_disk) => match mem_disk.get(key) {
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
