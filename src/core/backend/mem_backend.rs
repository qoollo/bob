use crate::core::backend::backend::*;
use crate::core::data::{BobData, BobKey, VDiskId, VDiskMapper};
use futures::future::{err, ok, Future};
use futures03::{compat::Future01CompatExt, future::err as err2, FutureExt};
use futures_locks::RwLock;
use std::{collections::HashMap, sync::Arc};

#[derive(Clone)]
struct VDisk {
    repo: Arc<RwLock<HashMap<BobKey, BobData>>>,
}

impl VDisk {
    pub fn new() -> VDisk {
        VDisk {
            repo: Arc::new(RwLock::new(HashMap::<BobKey, BobData>::new())),
        }
    }

    fn put(&self, key: BobKey, data: BobData) -> Put {
        Put({
            trace!("PUT[{}] to vdisk", key);
            self.repo
                .write()
                .then(move |disks_lock_res| match disks_lock_res {
                    Ok(mut repo) => {
                        repo.insert(key, data);
                        ok(BackendPutResult {})
                    }
                    Err(_) => err(BackendError::Other),
                })
                .compat()
                .boxed()
        })
    }

    fn get(&self, key: BobKey) -> Get {
        Get(self
            .repo
            .read()
            .then(move |repo_lock_res| match repo_lock_res {
                Ok(repo) => match repo.get(&key) {
                    Some(data) => {
                        trace!("GET[{}] from vdisk", key);
                        ok(BackendGetResult { data: data.clone() })
                    }
                    None => {
                        trace!("GET[{}] from vdisk failed. Cannot find key", key);
                        err(BackendError::NotFound)
                    }
                },
                Err(_) => err(BackendError::Other),
            })
            .compat()
            .boxed())
    }
}

#[derive(Clone)]
struct MemDisk {
    name: String,
    vdisks: HashMap<VDiskId, VDisk>,
}

impl MemDisk {
    pub fn new_direct(name: String, vdisks_count: u32) -> MemDisk {
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
        let b: HashMap<VDiskId, VDisk> = mapper
            .get_vdisks_by_disk(&name)
            .iter()
            .map(|id| (id.clone(), VDisk::new()))
            .collect::<HashMap<VDiskId, VDisk>>();
        MemDisk {
            name: name.clone(),
            vdisks: b,
        }
    }

    pub fn get(&self, vdisk_id: VDiskId, key: BobKey) -> Get {
        Get(match self.vdisks.get(&vdisk_id) {
            Some(vdisk) => {
                trace!(
                    "GET[{}] from vdisk: {} for disk: {}",
                    key,
                    vdisk_id,
                    self.name
                );
                vdisk.get(key).0
            }
            None => {
                trace!(
                    "GET[{}] from vdisk: {} failed. Cannot find vdisk for disk: {}",
                    key,
                    vdisk_id,
                    self.name
                );
                err2(BackendError::Other).boxed()
            }
        })
    }

    pub fn put(&self, vdisk_id: VDiskId, key: BobKey, data: BobData) -> Put {
        Put({
            match self.vdisks.get(&vdisk_id) {
                Some(vdisk) => {
                    trace!(
                        "PUT[{}] to vdisk: {} for disk: {}",
                        key,
                        vdisk_id,
                        self.name
                    );
                    vdisk.put(key, data).0
                }
                None => {
                    trace!(
                        "PUT[{}] to vdisk: {} failed. Cannot find vdisk for disk: {}",
                        key,
                        vdisk_id,
                        self.name
                    );
                    err2(BackendError::Other).boxed()
                }
            }
        })
    }
}

#[derive(Clone)]
pub struct MemBackend {
    disks: HashMap<String, MemDisk>,
    foreign_data: MemDisk,
}

impl MemBackend {
    pub fn new_direct(paths: &[String], vdisks_count: u32) -> MemBackend {
        let b = paths
            .iter()
            .map(|p| (p.clone(), MemDisk::new_direct(p.clone(), vdisks_count)))
            .collect::<HashMap<String, MemDisk>>();
        MemBackend {
            disks: b,
            foreign_data: MemDisk::new_direct("foreign".to_string(), vdisks_count),
        }
    }

    pub fn new(mapper: &VDiskMapper) -> MemBackend {
        let b = mapper
            .local_disks()
            .iter()
            .map(|node_disk| {
                (
                    node_disk.name.clone(),
                    MemDisk::new(node_disk.name.clone(), mapper),
                )
            })
            .collect::<HashMap<String, MemDisk>>();
        MemBackend {
            disks: b,
            foreign_data: MemDisk::new_direct("foreign".to_string(), mapper.vdisks_count()),
        }
    }
}

impl BackendStorage for MemBackend {
    fn put(&self, disk_name: String, vdisk: VDiskId, key: BobKey, data: BobData) -> Put {
        debug!("PUT[{}][{}] to backend", key, disk_name);
        let disk = self.disks.get(&disk_name).clone();
        Put(match disk {
            Some(mem_disk) => mem_disk.put(vdisk, key, data).0,
            None => {
                error!("PUT[{}][{}] Can't find disk {}", key, disk_name, disk_name);
                err2(BackendError::Other).boxed()
            }
        })
    }

    fn put_alien(&self, vdisk: VDiskId, key: BobKey, data: BobData) -> Put {
        debug!("PUT[{}] to backend, foreign data", key);
        Put(self.foreign_data.put(vdisk, key, data).0)
    }

    fn get(&self, disk_name: String, vdisk: VDiskId, key: BobKey) -> Get {
        debug!("GET[{}][{}] to backend", key, disk_name);
        Get(match self.disks.get(&disk_name) {
            Some(mem_disk) => mem_disk.get(vdisk, key).0,
            None => {
                error!("GET[{}][{}] Can't find disk {}", key, disk_name, disk_name);
                err2(BackendError::Other).boxed()
            }
        })
    }

    fn get_alien(&self, vdisk: VDiskId, key: BobKey) -> Get {
        debug!("GET[{}] to backend, foreign data", key);
        Get(self.foreign_data.get(vdisk, key).0)
    }
}
