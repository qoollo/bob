use crate::prelude::*;

use crate::core::{BackendStorage, MetricsProducer, Operation};

#[derive(Clone, Debug, Default)]
pub struct VDisk {
    inner: Arc<SyncRwLock<HashMap<BobKey, BobData>>>,
}

impl VDisk {
    async fn put(&self, key: BobKey, data: &BobData) -> Result<(), Error> {
        debug!("PUT[{}] to vdisk", key);
        self.inner
            .write()
            .expect("rwlock")
            .insert(key, data.clone());
        Ok(())
    }

    async fn get(&self, key: BobKey) -> Result<BobData, Error> {
        if let Some(data) = self.inner.read().expect("rwlock").get(&key) {
            debug!("GET[{}] from vdisk", key);
            Ok(data.clone())
        } else {
            debug!("GET[{}] from vdisk failed. Cannot find key", key);
            Err(Error::key_not_found(key))
        }
    }

    async fn exist(&self, keys: &[BobKey]) -> Result<Vec<bool>, Error> {
        let repo = self.inner.read().expect("rwlock");
        let result = keys.iter().map(|k| repo.get(k).is_some()).collect();
        Ok(result)
    }

    async fn delete(&self, key: BobKey) -> Result<u64, Error> {
        if self.inner.write().expect("rwlock").remove(&key).is_some() {
            debug!("DELETE[{}] from vdisk", key);
            Ok(1)
        } else {
            debug!("DELETE[{}] from vdisk failed. Cannot find key", key);
            Err(Error::key_not_found(key))
        }
    }
}

#[derive(Clone, Debug)]
pub struct MemDisk {
    pub name: DiskName,
    pub vdisks: HashMap<VDiskId, VDisk>,
}

impl MemDisk {
    pub fn new_direct(name: DiskName, vdisks_count: u32) -> Self {
        let vdisks = (0..vdisks_count).map(|i| (i, VDisk::default())).collect();
        Self { name, vdisks }
    }

    pub fn new(name: DiskName, mapper: &Virtual) -> Self {
        let vdisks = mapper
            .get_vdisks_by_disk(&name)
            .iter()
            .map(|id| (*id, VDisk::default()))
            .collect();
        Self { name, vdisks }
    }

    pub async fn get(&self, vdisk_id: VDiskId, key: BobKey) -> Result<BobData, Error> {
        if let Some(vdisk) = self.vdisks.get(&vdisk_id) {
            debug!("GET[{}] from: {} for disk: {}", key, vdisk_id, self.name);
            debug!("{:?}", *vdisk.inner.read().expect("rwlock"));
            vdisk.get(key).await
        } else {
            debug!("GET[{}] Cannot find vdisk for disk: {}", key, self.name);
            Err(Error::internal())
        }
    }

    pub async fn put(&self, vdisk_id: VDiskId, key: BobKey, data: &BobData) -> Result<(), Error> {
        if let Some(vdisk) = self.vdisks.get(&vdisk_id) {
            debug!("PUT[{}] to vdisk: {} for: {}", key, vdisk_id, self.name);
            vdisk.put(key, data).await
        } else {
            debug!("PUT[{}] Cannot find vdisk for disk: {}", key, self.name);
            Err(Error::internal())
        }
    }

    pub async fn exist(&self, vdisk_id: VDiskId, keys: &[BobKey]) -> Result<Vec<bool>, Error> {
        if let Some(vdisk) = self.vdisks.get(&vdisk_id) {
            trace!("EXIST from vdisk: {} for disk: {}", vdisk_id, self.name);
            vdisk.exist(keys).await
        } else {
            trace!("EXIST Cannot find vdisk for disk: {}", self.name);
            Err(Error::internal())
        }
    }

    pub async fn delete(&self, vdisk_id: VDiskId, key: BobKey) -> Result<u64, Error> {
        if let Some(vdisk) = self.vdisks.get(&vdisk_id) {
            debug!("DELETE[{}] from: {} for disk: {}", key, vdisk_id, self.name);
            vdisk.delete(key).await
        } else {
            debug!("DELETE[{}] Cannot find vdisk for disk: {}", key, self.name);
            Err(Error::internal())
        }
    }
}

#[derive(Clone, Debug)]
pub struct MemBackend {
    pub disks: HashMap<DiskName, MemDisk>,
    pub foreign_data: MemDisk,
}

impl MemBackend {
    pub fn new(mapper: &Virtual) -> Self {
        let disks = mapper
            .local_disks()
            .iter()
            .map(DiskPath::name)
            .map(|name| (name.clone(), MemDisk::new(name.clone(), mapper)))
            .collect();
        Self {
            disks,
            foreign_data: MemDisk::new_direct("foreign".into(), mapper.vdisks_count()),
        }
    }
}

#[async_trait]
impl MetricsProducer for MemBackend {}

#[async_trait]
impl BackendStorage for MemBackend {
    async fn run_backend(&self) -> AnyResult<()> {
        debug!("run mem backend");
        Ok(())
    }

    async fn put(&self, op: Operation, key: BobKey, data: &BobData) -> Result<(), Error> {
        let disk_name = op.disk_name_local();
        debug!("PUT[{}][{}] to backend", key, disk_name);
        let disk = self.disks.get(&disk_name);
        if let Some(mem_disk) = disk {
            mem_disk.put(op.vdisk_id(), key, data).await
        } else {
            error!("PUT[{}] Can't find disk {}", key, disk_name);
            Err(Error::internal())
        }
    }

    async fn put_alien(&self, op: Operation, key: BobKey, data: &BobData) -> Result<(), Error> {
        debug!("PUT[{}] to backend, foreign data", key);
        self.foreign_data.put(op.vdisk_id(), key, data).await
    }

    async fn get(&self, op: Operation, key: BobKey) -> Result<BobData, Error> {
        debug!("GET[{}][{}] to backend", key, op.disk_name_local());
        if let Some(mem_disk) = self.disks.get(&op.disk_name_local()) {
            mem_disk.get(op.vdisk_id(), key).await
        } else {
            error!("GET[{}] Can't find disk {}", key, op.disk_name_local());
            Err(Error::internal())
        }
    }

    async fn get_alien(&self, op: Operation, key: BobKey) -> Result<BobData, Error> {
        debug!("GET[{}] to backend, foreign data", key);
        debug!("{:?}", self.foreign_data);
        self.foreign_data.get(op.vdisk_id(), key).await
    }

    async fn exist(&self, op: Operation, keys: &[BobKey]) -> Result<Vec<bool>, Error> {
        debug!("EXIST[{}] to backend", op.disk_name_local());

        if let Some(mem_disk) = self.disks.get(&op.disk_name_local()) {
            mem_disk.exist(op.vdisk_id(), keys).await
        } else {
            error!("EXIST Can't find disk {}", op.disk_name_local());
            Err(Error::internal())
        }
    }

    async fn exist_alien(&self, operation: Operation, keys: &[BobKey]) -> Result<Vec<bool>, Error> {
        debug!("EXIST to backend, foreign data");
        self.foreign_data.exist(operation.vdisk_id(), keys).await
    }

    async fn delete(&self, op: Operation, key: BobKey, _meta: &BobMeta) -> Result<u64, Error> {
        debug!("DELETE[{}][{}] from backend", key, op.disk_name_local());
        if let Some(mem_disk) = self.disks.get(&op.disk_name_local()) {
            mem_disk.delete(op.vdisk_id(), key).await
        } else {
            error!("DELETE[{}] Can't find disk {}", key, op.disk_name_local());
            Err(Error::internal())
        }
    }

    async fn delete_alien(&self, op: Operation, key: BobKey, _meta: &BobMeta, _force_delete: bool) -> Result<u64, Error> {
        debug!("DELETE[{}] from backend, foreign data", key);
        debug!("{:?}", self.foreign_data);
        self.foreign_data.delete(op.vdisk_id(), key).await
    }

    async fn shutdown(&self) {}
}
