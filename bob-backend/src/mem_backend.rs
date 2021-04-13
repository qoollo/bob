use crate::prelude::*;

use crate::core::{BackendStorage, Operation};

#[derive(Clone, Debug, Default)]
pub struct VDisk {
    inner: Arc<RwLock<HashMap<BobKey, BobData>>>,
}

impl VDisk {
    async fn put(&self, key: BobKey, data: BobData) -> Result<(), Error> {
        debug!("PUT[{}] to vdisk", key);
        self.inner.write().await.insert(key, data);
        Ok(())
    }

    async fn get(&self, key: BobKey) -> Result<BobData, Error> {
        if let Some(data) = self.inner.read().await.get(&key) {
            debug!("GET[{}] from vdisk", key);
            Ok(data.clone())
        } else {
            debug!("GET[{}] from vdisk failed. Cannot find key", key);
            Err(Error::key_not_found(key))
        }
    }

    async fn exist(&self, keys: &[BobKey]) -> Result<Vec<bool>, Error> {
        let repo = self.inner.read().await;
        let result = keys.iter().map(|k| repo.get(k).is_some()).collect();
        Ok(result)
    }
}

#[derive(Clone, Debug)]
pub struct MemDisk {
    pub name: String,
    pub vdisks: HashMap<VDiskId, VDisk>,
}

impl MemDisk {
    pub fn new_direct(name: String, vdisks_count: u32) -> Self {
        let vdisks = (0..vdisks_count).map(|i| (i, VDisk::default())).collect();
        Self { name, vdisks }
    }

    pub fn new(name: String, mapper: &Virtual) -> Self {
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
            debug!("{:?}", *vdisk.inner.read().await);
            vdisk.get(key).await
        } else {
            debug!("GET[{}] Cannot find vdisk for disk: {}", key, self.name);
            Err(Error::internal())
        }
    }

    pub async fn put(&self, vdisk_id: VDiskId, key: BobKey, data: BobData) -> Result<(), Error> {
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
}

#[derive(Clone, Debug)]
pub struct MemBackend {
    pub disks: HashMap<String, MemDisk>,
    pub foreign_data: MemDisk,
}

impl MemBackend {
    pub fn new(mapper: &Virtual) -> Self {
        let disks = mapper
            .local_disks()
            .iter()
            .map(DiskPath::name)
            .map(|name| (name.to_string(), MemDisk::new(name.to_string(), &mapper)))
            .collect();
        Self {
            disks,
            foreign_data: MemDisk::new_direct("foreign".to_string(), mapper.vdisks_count()),
        }
    }
}

#[async_trait]
impl BackendStorage for MemBackend {
    async fn run_backend(&self) -> AnyResult<()> {
        debug!("run mem backend");
        Ok(())
    }

    async fn put(&self, op: Operation, key: BobKey, data: BobData) -> Result<(), Error> {
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

    async fn put_alien(&self, op: Operation, key: BobKey, data: BobData) -> Result<(), Error> {
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

    async fn shutdown(&self) {}
}
