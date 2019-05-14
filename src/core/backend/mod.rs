pub mod mem_backend;
pub mod mem_tests;
pub mod stub_backend;
use crate::core::data::{BackendOperation, BobData, BobKey,VDiskId};
use tokio::prelude::Future;
use crate::core::configs::node::{BackendType};
use crate::core::data::VDiskMapper;
use crate::core::backend::stub_backend::StubBackend;
use crate::core::backend::mem_backend::MemBackend;

#[derive(Debug)]
pub struct BackendResult {}

#[derive(Debug, PartialEq)]
pub enum BackendError {
    NotFound,
    Other,
    __Nonexhaustive,
}

pub struct BackendGetResult {
    pub data: BobData,
}
pub type BackendPutFuture = Box<Future<Item = BackendResult, Error = BackendError> + Send>;
pub type BackendGetFuture = Box<Future<Item = BackendGetResult, Error = BackendError> + Send>;

pub trait Backend {
    fn put(&self, disk: String, vdisk: VDiskId, key: BobKey, data: BobData) -> BackendPutFuture;
    fn put_alien(&self, vdisk: VDiskId, key: BobKey, data: BobData) -> BackendPutFuture;

    fn get(&self, disk: String, vdisk: VDiskId, key: BobKey) -> BackendGetFuture;
    fn get_alien(&self, vdisk: VDiskId, key: BobKey) -> BackendGetFuture;
}

pub struct BackendStruct {
    pub backend: Box<dyn Backend + Send + Sync>,
}

impl BackendStruct {
    pub fn new(mapper: &VDiskMapper, backend_type: &BackendType) -> Self {
        let backend: Box<Backend + Send + Sync + 'static> = match backend_type {
            BackendType::InMemory => Box::new(MemBackend::new(mapper)),
            BackendType::Stub => Box::new(StubBackend {}),
        };
        BackendStruct {
            backend
        }
    }

    pub fn put(&self, op: &BackendOperation, key: BobKey, data: BobData) -> BackendPutFuture {
        let id = op.vdisk_id.clone();

        if !op.is_data_alien() {
            let disk = op.disk_name_local();
            debug!("PUT[{}][{}] to backend", key, disk);
            self.backend.put(disk, id, key, data)
        } else {
            debug!("PUT[{}] to backend, foreign data", key);
            self.backend.put_alien(id, key, data)
        }
    }

    pub fn get(&self, op: &BackendOperation, key: BobKey) -> BackendGetFuture {
        let id = op.vdisk_id.clone();

        if !op.is_data_alien() {
            let disk = op.disk_name_local();
            debug!("GET[{}][{}] to backend", key, disk);
            self.backend.get(disk, id, key)
        } else {
            debug!("GET[{}] to backend, foreign data", key);
            self.backend.get_alien(id, key)
        }
    }
}