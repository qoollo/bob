use super::prelude::*;
use crate::core::backend::core::Exist;

#[derive(Clone, Debug)]
pub(crate) struct VDisk {
    repo: Arc<RwLock<HashMap<BobKey, BobData>>>,
}

impl VDisk {
    pub(crate) fn new() -> Self {
        Self {
            repo: Arc::new(RwLock::new(HashMap::<BobKey, BobData>::new())),
        }
    }

    fn put(&self, key: BobKey, data: BobData) -> Put {
        let repo = self.repo.clone();
        Put(Box::pin(async move {
            trace!("PUT[{}] to vdisk", key);
            let mut repo = repo.write().await;
            repo.insert(key, data);
            Ok(BackendPutResult {})
        }))
    }

    fn get(&self, key: BobKey) -> Get {
        let repo = self.repo.clone();
        Get(Box::pin(async move {
            let repo = repo.read().await;
            if let Some(data) = repo.get(&key) {
                trace!("GET[{}] from vdisk", key);
                Ok(BackendGetResult { data: data.clone() })
            } else {
                trace!("GET[{}] from vdisk failed. Cannot find key", key);
                Err(Error::KeyNotFound(key))
            }
        }))
    }

    fn exist(&self, keys: &[BobKey]) -> Exist {
        let keys = keys.to_vec();
        let repo = self.repo.clone();
        Exist(Box::pin(async move {
            let repo = repo.read().await;
            let result = keys.iter().map(|k| repo.get(k).is_some()).collect();
            Ok(BackendExistResult { exist: result })
        }))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MemDisk {
    pub(crate) name: String,
    pub(crate) vdisks: HashMap<VDiskId, VDisk>,
}

impl MemDisk {
    pub(crate) fn new_direct(name: String, vdisks_count: u32) -> Self {
        let mut vdisks = HashMap::new();
        for i in 0..vdisks_count {
            vdisks.insert(i, VDisk::new());
        }
        Self { name, vdisks }
    }

    pub(crate) fn new(name: String, mapper: &Virtual) -> Self {
        let vdisks = mapper
            .get_vdisks_by_disk(&name)
            .iter()
            .map(|id| (id.clone(), VDisk::new()))
            .collect::<HashMap<_, _>>();
        Self { name, vdisks }
    }

    pub(crate) fn get(&self, vdisk_id: &VDiskId, key: BobKey) -> Get {
        Get(if let Some(vdisk) = self.vdisks.get(&vdisk_id) {
            trace!(
                "GET[{}] from vdisk: {} for disk: {}",
                key,
                vdisk_id,
                self.name
            );
            vdisk.get(key).0
        } else {
            trace!(
                "GET[{}] from vdisk: {} failed. Cannot find vdisk for disk: {}",
                key,
                vdisk_id,
                self.name
            );
            future::err(Error::Internal).boxed()
        })
    }

    pub(crate) fn put(&self, vdisk_id: &VDiskId, key: BobKey, data: BobData) -> Put {
        Put({
            if let Some(vdisk) = self.vdisks.get(&vdisk_id) {
                trace!(
                    "PUT[{}] to vdisk: {} for disk: {}",
                    key,
                    vdisk_id,
                    self.name
                );
                vdisk.put(key, data).0
            } else {
                trace!(
                    "PUT[{}] to vdisk: {} failed. Cannot find vdisk for disk: {}",
                    key,
                    vdisk_id,
                    self.name
                );
                future::err(Error::Internal).boxed()
            }
        })
    }

    pub(crate) fn exist(&self, vdisk_id: &VDiskId, keys: &[BobKey]) -> Exist {
        Exist(if let Some(vdisk) = self.vdisks.get(&vdisk_id) {
            trace!("EXIST from vdisk: {} for disk: {}", vdisk_id, self.name);
            vdisk.exist(keys).0
        } else {
            trace!(
                "EXIST from vdisk: {} failed. Cannot find vdisk for disk: {}",
                vdisk_id,
                self.name
            );
            future::err(Error::Internal).boxed()
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MemBackend {
    pub(crate) disks: HashMap<String, MemDisk>,
    pub(crate) foreign_data: MemDisk,
}

impl MemBackend {
    pub(crate) fn new(mapper: &Virtual) -> Self {
        let disks = mapper
            .local_disks()
            .iter()
            .map(|node_disk| {
                (
                    node_disk.name().to_owned(),
                    MemDisk::new(node_disk.name().to_owned(), &mapper),
                )
            })
            .collect::<HashMap<_, _>>();
        Self {
            disks,
            foreign_data: MemDisk::new_direct("foreign".to_string(), mapper.vdisks_count()),
        }
    }
}

impl BackendStorage for MemBackend {
    fn run_backend(&self) -> RunResult {
        future::ok(()).boxed()
    }

    fn put(&self, operation: BackendOperation, key: BobKey, data: BobData) -> Put {
        debug!("PUT[{}][{}] to backend", key, operation.disk_name_local());
        let disk = self.disks.get(&operation.disk_name_local());
        Put(if let Some(mem_disk) = disk {
            mem_disk.put(&operation.vdisk_id, key, data).0
        } else {
            error!(
                "PUT[{}] Can't find disk {}",
                key,
                operation.disk_name_local()
            );
            future::err(Error::Internal).boxed()
        })
    }

    fn put_alien(&self, operation: BackendOperation, key: BobKey, data: BobData) -> Put {
        debug!("PUT[{}] to backend, foreign data", key);
        Put(self.foreign_data.put(&operation.vdisk_id, key, data).0)
    }

    fn get(&self, operation: BackendOperation, key: BobKey) -> Get {
        debug!("GET[{}][{}] to backend", key, operation.disk_name_local());
        Get(
            if let Some(mem_disk) = self.disks.get(&operation.disk_name_local()) {
                mem_disk.get(&operation.vdisk_id, key).0
            } else {
                error!(
                    "GET[{}] Can't find disk {}",
                    key,
                    operation.disk_name_local()
                );
                future::err(Error::Internal).boxed()
            },
        )
    }

    fn get_alien(&self, operation: BackendOperation, key: BobKey) -> Get {
        debug!("GET[{}] to backend, foreign data", key);
        Get(self.foreign_data.get(&operation.vdisk_id, key).0)
    }

    fn exist(&self, operation: BackendOperation, keys: &[BobKey]) -> Exist {
        debug!("EXIST[{}] to backend", operation.disk_name_local());
        Exist(
            if let Some(mem_disk) = self.disks.get(&operation.disk_name_local()) {
                mem_disk.exist(&operation.vdisk_id, keys).0
            } else {
                error!("EXIST Can't find disk {}", operation.disk_name_local());
                future::err(Error::Internal).boxed()
            },
        )
    }

    fn exist_alien(&self, operation: BackendOperation, keys: &[BobKey]) -> Exist {
        debug!("EXIST to backend, foreign data");
        Exist(self.foreign_data.exist(&operation.vdisk_id, keys).0)
    }
}
