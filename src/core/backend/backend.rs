use crate::core::backend::{
    mem_backend::MemBackend, pearl_backend::PearlBackend, stub_backend::StubBackend,
};
use crate::core::configs::node::{BackendType, NodeConfig};
use crate::core::data::{BobData, BobKey, DiskPath, VDiskId, VDiskMapper};
use futures03::{
    future::{FutureExt, TryFutureExt},
    Future,
};
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct BackendOperation {
    vdisk_id: VDiskId,
    disk_path: Option<DiskPath>,
    alien: bool, // flag marks data belonging for different node
}

impl std::fmt::Display for BackendOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self.disk_path.clone() {
            Some(path) => write!(
                f,
                "#{}-{}-{}-{}",
                self.vdisk_id, path.name, path.path, self.alien
            ),
            None => write!(f, "#{}-{}", self.vdisk_id, self.alien),
        }
    }
}

impl BackendOperation {
    pub fn new_alien(vdisk_id: VDiskId) -> BackendOperation {
        BackendOperation {
            vdisk_id,
            disk_path: None,
            alien: true,
        }
    }
    pub fn new_local(vdisk_id: VDiskId, path: DiskPath) -> BackendOperation {
        BackendOperation {
            vdisk_id,
            disk_path: Some(path),
            alien: false,
        }
    }
    pub fn is_data_alien(&self) -> bool {
        self.alien
    }
    pub fn disk_name_local(&self) -> String {
        self.disk_path.clone().unwrap().name.clone()
    }
}

#[derive(Debug)]
pub struct BackendResult {}

#[derive(Debug, PartialEq)]
pub enum BackendError {
    NotFound,
    VDiskNotFound,
    StorageError,
    Other,
    __Nonexhaustive,
}
impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

pub struct BackendGetResult {
    pub data: BobData,
}

pub struct Put(pub Pin<Box<dyn Future<Output = Result<BackendResult, BackendError>> + Send>>);
pub struct Get(pub Pin<Box<dyn Future<Output = Result<BackendGetResult, BackendError>> + Send>>);

pub trait BackendStorage {
    fn put(&self, disk_name: String, vdisk: VDiskId, key: BobKey, data: BobData) -> Put;
    fn put_alien(&self, vdisk: VDiskId, key: BobKey, data: BobData) -> Put;

    fn get(&self, disk_name: String, vdisk: VDiskId, key: BobKey) -> Get;
    fn get_alien(&self, vdisk: VDiskId, key: BobKey) -> Get;
}

pub struct Backend {
    pub backend: Arc<dyn BackendStorage + Send + Sync>,
}

impl Backend {
    pub fn new(mapper: &VDiskMapper, config: &NodeConfig) -> Self {
        let backend: Arc<dyn BackendStorage + Send + Sync + 'static> = match config.backend_type() {
            BackendType::InMemory => Arc::new(MemBackend::new(mapper)),
            BackendType::Stub => Arc::new(StubBackend {}),
            BackendType::Pearl => {
                let mut pearl = PearlBackend::new(config);
                pearl.init(mapper).unwrap();
                Arc::new(pearl)
            }
        };
        Backend { backend }
    }

    pub fn put(&self, op: &BackendOperation, key: BobKey, data: BobData) -> Put {
        Put({
            let oper = op.clone();
            let backend = self.backend.clone();

            if !oper.is_data_alien() {
                debug!("PUT[{}][{}] to backend", key, oper.disk_name_local());
                let result = backend
                    .put(
                        oper.disk_name_local(),
                        oper.vdisk_id.clone(),
                        key,
                        data.clone(),
                    )
                    .0;
                let func = move |err| {
                    error!(
                        "PUT[{}][{}] to backend. Error: {}",
                        key,
                        oper.disk_name_local(),
                        err
                    );
                    backend.put_alien(oper.vdisk_id.clone(), key, data).0
                };

                result.or_else(|err| func(err)).boxed()
            } else {
                debug!(
                    "PUT[{}] to backend, alien data for {}",
                    key,
                    op.vdisk_id.clone()
                );
                backend.put_alien(op.vdisk_id.clone(), key, data).0
            }
        })
    }

    pub fn get(&self, op: &BackendOperation, key: BobKey) -> Get {
        Get({
            let oper = op.clone();
            let backend = self.backend.clone();

            if !oper.is_data_alien() {
                debug!("GET[{}][{}] to backend", key, oper.disk_name_local());
                backend
                    .get(oper.disk_name_local(), oper.vdisk_id.clone(), key)
                    .0
            } else {
                debug!("GET[{}] to backend, foreign data", key);
                backend.get_alien(oper.vdisk_id.clone(), key).0
            }
        })
    }
}
