use crate::core::backend::*;
use crate::core::data::{BobData, BobKey, VDiskId, VDiskMapper, WriteOption};
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
        trace!("PUT[{}] to vdisk", key);
        self.repo.insert(key, data);
    }

    pub fn get(&self, key: BobKey) -> Option<BobData> {
        match self.repo.get(&key) {
            Some(data) => {
                trace!("GET[{}] from vdisk", key);
                Some(data.clone())
            },
            None => {
                trace!("GET[{}] from vdisk failed. Cannot find key", key);
                None
            },
        }
    }
}

struct MemDisk {
    name: String,
    vdisks: HashMap<VDiskId, VDisk>,
}

impl MemDisk {
    pub fn new_test(name: String, vdisks_count: u32) -> MemDisk {
        let mut b: HashMap<VDiskId, VDisk> = HashMap::new();
        for i in 0..vdisks_count {
            b.insert(VDiskId::new(i), VDisk::new());
        }
        MemDisk {
            name: name.clone(),
            vdisks: b,
        }
    }

    pub fn new(name: String, mapper: &VDiskMapper) -> MemDisk {
        let b: HashMap<VDiskId, VDisk> =  mapper.get_vdisks_by_disk(&name)
            .iter()
            .map(|id|(id.clone(), VDisk::new()))
            .collect::<HashMap<VDiskId, VDisk>>();
        MemDisk {
            name: name.clone(),
            vdisks: b
        }
    }

    pub fn put(&mut self, vdisk_id: VDiskId, key: BobKey, data: BobData) {
        match self.vdisks.get_mut(&vdisk_id) {
            Some(vdisk) => {
                trace!("PUT[{}] to vdisk: {} for disk: {}", key, vdisk_id, self.name);
                vdisk.put(key, data)
            },
            None => {
                trace!("PUT[{}] to vdisk: {} failed. Cannot find vdisk for disk: {}", key, vdisk_id, self.name);
            },
        }
    }

    pub fn get(&self, vdisk_id: VDiskId, key: BobKey) -> Option<BobData> {
        match self.vdisks.get(&vdisk_id) {
            Some(vdisk) => {
                trace!("GET[{}] from vdisk: {} for disk: {}", key, vdisk_id, self.name);
                vdisk.get(key)
            },
            None => {
                trace!("GET[{}] from vdisk: {} failed. Cannot find vdisk for disk: {}", key, vdisk_id, self.name);
                None
            },
        }
    }
}

#[derive(Clone)]
pub struct MemBackend {
    disks: Arc<RwLock<HashMap<String, MemDisk>>>,
}

impl MemBackend {
    pub fn new_test(paths: &[String], vdisks_count: u32) -> MemBackend {
        let b = paths.iter()
            .map(|p|(p.clone(), MemDisk::new_test(p.clone(), vdisks_count)))
            .collect::<HashMap<String, MemDisk>>();
        MemBackend {
            disks: Arc::new(RwLock::new(b)),
        }
    }

    pub fn new(mapper: &VDiskMapper) -> MemBackend {
        let b = mapper.local_disks().iter()
            .map(|node_disk|(node_disk.name.clone(), MemDisk::new(node_disk.name.clone(), mapper)))
            .collect::<HashMap<String, MemDisk>>();
        MemBackend {
            disks: Arc::new(RwLock::new(b)),
        }
    }
}

impl Backend for MemBackend {
    fn put(&self, op: &WriteOption, key: BobKey, data: BobData) -> BackendPutFuture {
        let disk = op.disk_name.clone();
        let id = op.vdisk_id.clone();

        debug!("PUT[{}][{}]", key, disk);
        Box::new(self
            .disks
            .write()
            .then(move |disks_lock_res| match disks_lock_res {
                Ok(mut disks) => match disks.get_mut(&disk) {
                    Some(mem_disk) => {
                        mem_disk.put(id, key, data);
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
    fn get(&self, op: &WriteOption, key: BobKey) -> BackendGetFuture {
        let disk = op.disk_name.clone();
        let id = op.vdisk_id.clone();

        debug!("GET[{}][{}]", key, disk);
        Box::new(self
            .disks
            .read()
            .then(move |disks_lock_res| match disks_lock_res {
                Ok(disks) => match disks.get(&disk) {
                    Some(mem_disk) => match mem_disk.get(id, key) {
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
