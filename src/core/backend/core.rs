use crate::core::{
    backend::{
        mem_backend::MemBackend, pearl::core::PearlBackend, stub_backend::StubBackend, Error,
    },
    configs::node::{BackendType, NodeConfig},
    data::{BobData, BobKey, DiskPath, VDiskId, BobOptions},
    mapper::VDiskMapper,
};
use futures03::{
    future::{ready, FutureExt, TryFutureExt},
    task::Spawn,
    Future,
};
use std::{pin::Pin, sync::Arc};

#[derive(Debug, Clone)]
pub struct BackendOperation {
    vdisk_id: VDiskId,
    disk_path: Option<DiskPath>,
    remote_node_name: Option<String>, // save data to alien/<remote_node_name>
    alien: bool,                      // flag marks data belonging for different node
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
            remote_node_name: None,
            alien: true,
        }
    }
    pub fn new_local(vdisk_id: VDiskId, path: DiskPath) -> BackendOperation {
        BackendOperation {
            vdisk_id,
            disk_path: Some(path),
            remote_node_name: None,
            alien: false,
        }
    }

    pub fn set_remote_folder(&mut self, name: &str) {
        //TODO fix
        self.remote_node_name = Some(name.to_string())
    }
    pub fn is_data_alien(&self) -> bool {
        self.alien
    }
    pub fn disk_name_local(&self) -> String {
        self.disk_path.clone().unwrap().name.clone()
    }
}

#[derive(Debug)]
pub struct BackendPutResult {}

#[derive(Debug)]
pub struct BackendGetResult {
    pub data: BobData,
}

#[derive(Debug)]
pub struct BackendPingResult {}

pub type GetResult = Result<BackendGetResult, Error>;
pub struct Get(pub Pin<Box<dyn Future<Output = GetResult> + Send>>);

pub type PutResult = Result<BackendPutResult, Error>;
pub struct Put(pub Pin<Box<dyn Future<Output = PutResult> + Send>>);

pub type RunResult = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;

pub trait BackendStorage {
    fn run_backend(&self) -> RunResult;

    fn put(&self, disk_name: String, vdisk: VDiskId, key: BobKey, data: BobData) -> Put;
    fn put_alien(&self, vdisk: VDiskId, key: BobKey, data: BobData) -> Put;

    fn get(&self, disk_name: String, vdisk: VDiskId, key: BobKey) -> Get;
    fn get_alien(&self, vdisk: VDiskId, key: BobKey) -> Get;
}

pub struct Backend {
    backend: Arc<dyn BackendStorage + Send + Sync>,
    mapper: Arc<VDiskMapper>,
}

impl Backend {
    pub fn new<TSpawner: Spawn + Clone + Send + 'static + Unpin + Sync>(
        mapper: Arc<VDiskMapper>,
        config: &NodeConfig,
        spawner: TSpawner,
    ) -> Self {
        let backend: Arc<dyn BackendStorage + Send + Sync + 'static> = match config.backend_type() {
            BackendType::InMemory => Arc::new(MemBackend::new(mapper.clone())),
            BackendType::Stub => Arc::new(StubBackend {}),
            BackendType::Pearl => Arc::new(PearlBackend::new(mapper.clone(), config, spawner)),
        };
        Backend { backend, mapper }
    }

    pub async fn run_backend(&self) -> Result<(), String> {
        self.backend.run_backend().boxed().await
    }

    pub fn put(&self, key: BobKey, data: BobData, _options: BobOptions) -> Put {
        let operation = self.mapper.get_operation(key);
        self.put_local(key, data, operation)
    }

    pub fn put_local(&self, key: BobKey, data: BobData, op: BackendOperation) -> Put {
        Put({
            let operation = op.clone();
            let backend = self.backend.clone();

            if !operation.is_data_alien() {
                debug!("PUT[{}][{}] to backend", key, operation.disk_name_local());
                let result = backend
                    .put(
                        operation.disk_name_local(),
                        operation.vdisk_id.clone(),
                        key,
                        data.clone(),
                    )
                    .0;
                let func = move |err| {
                    error!(
                        "PUT[{}][{}] to backend. Error: {:?}",
                        key,
                        operation.disk_name_local(),
                        err
                    );
                    backend.put_alien(operation.vdisk_id.clone(), key, data).0
                };

                result
                    .or_else(|err| {
                        if err != Error::DuplicateKey {
                            func(err)
                        } else {
                            ready(Err(err)).boxed()
                        }
                    })
                    .boxed()
            } else {
                debug!(
                    "PUT[{}] to backend, alien data for {}",
                    key,
                    operation.vdisk_id.clone()
                );
                backend.put_alien(operation.vdisk_id.clone(), key, data).0
            }
        })
    }

    pub fn get(&self, key: BobKey, _options: BobOptions) -> Get {
        Get({
            let operation = self.mapper.get_operation(key);
            let backend = self.backend.clone();

            if !operation.is_data_alien() {
                debug!("GET[{}][{}] to backend", key, operation.disk_name_local());
                backend
                    .get(operation.disk_name_local(), operation.vdisk_id.clone(), key)
                    .0
            } else {
                debug!("GET[{}] to backend, foreign data", key);
                backend.get_alien(operation.vdisk_id.clone(), key).0
            }
        })
    }
}
