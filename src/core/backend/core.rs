use crate::core::{
    backend::{
        mem_backend::MemBackend, pearl::core::PearlBackend, stub_backend::StubBackend, Error,
    },
    configs::node::{BackendType, NodeConfig},
    data::{BobData, BobKey, BobOptions, DiskPath, VDiskId},
    mapper::VDiskMapper,
};
use futures03::{future::FutureExt, task::Spawn, Future};
use std::{pin::Pin, sync::Arc};

#[derive(Debug, Clone)]
pub struct BackendOperation {
    pub vdisk_id: VDiskId,
    disk_path: Option<DiskPath>,
    remote_node_name: Option<String>, // save data to alien/<remote_node_name>
}

impl std::fmt::Display for BackendOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self.disk_path.clone() {
            Some(path) => write!(
                f,
                "[id: {}, name: {}, path: {}, alien: {}]",
                self.vdisk_id,
                path.name,
                path.path,
                self.is_data_alien()
            ),
            None => write!(
                f,
                "[id: {}, alien: {}]",
                self.vdisk_id,
                self.is_data_alien()
            ),
        }
    }
}

impl BackendOperation {
    pub fn new_alien(vdisk_id: VDiskId) -> BackendOperation {
        BackendOperation {
            vdisk_id,
            disk_path: None,
            remote_node_name: None,
        }
    }
    pub fn new_local(vdisk_id: VDiskId, path: DiskPath) -> BackendOperation {
        BackendOperation {
            vdisk_id,
            disk_path: Some(path),
            remote_node_name: None,
        }
    }
    pub fn clone_alien(&self) -> Self {
        BackendOperation {
            vdisk_id: self.vdisk_id.clone(),
            disk_path: None,
            remote_node_name: self.remote_node_name.clone(),
        }
    }
    pub fn set_remote_folder(&mut self, name: &str) {
        self.remote_node_name = Some(name.to_string())
    }
    pub fn is_data_alien(&self) -> bool {
        self.disk_path.is_none()
    }
    pub fn disk_name_local(&self) -> String {
        self.disk_path.clone().unwrap().name.clone()
    }
    pub fn remote_node_name(&self) -> String {
        self.remote_node_name.clone().unwrap()
    }
}

#[derive(Debug)]
pub struct BackendPutResult {}

#[derive(Debug)]
pub struct BackendGetResult {
    pub data: BobData,
}

impl std::fmt::Display for BackendGetResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[derive(Debug)]
pub struct BackendPingResult {}

pub type GetResult = Result<BackendGetResult, Error>;
pub struct Get(pub Pin<Box<dyn Future<Output = GetResult> + Send>>);

pub type PutResult = Result<BackendPutResult, Error>;
pub struct Put(pub Pin<Box<dyn Future<Output = PutResult> + Send>>);

pub type RunResult = Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>;

pub trait BackendStorage {
    fn run_backend(&self) -> RunResult;

    fn put(&self, operation: BackendOperation, key: BobKey, data: BobData) -> Put;
    fn put_alien(&self, operation: BackendOperation, key: BobKey, data: BobData) -> Put;

    fn get(&self, operation: BackendOperation, key: BobKey) -> Get;
    fn get_alien(&self, operation: BackendOperation, key: BobKey) -> Get;
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

    pub async fn run_backend(&self) -> Result<(), Error> {
        self.backend.run_backend().boxed().await
    }

    pub async fn put(&self, key: BobKey, data: BobData, options: BobOptions) -> PutResult {
        let (vdisk_id, disk_path) = self.mapper.get_operation(key);
        let result = if options.have_remote_node() {
            let mut result = Ok(BackendPutResult {});
            // write to all remote_nodes
            for node_name in options.remote_nodes.iter() {
                let mut op = BackendOperation::new_alien(vdisk_id.clone());
                op.set_remote_folder(node_name);

                //TODO make it parallel?
                if let Err(err) = self.put_single(key, data.clone(), op).await {
                    //TODO stop after first error?
                    result = Err(err);
                    break;
                }
            }
            result
        } else if let Some(path) = disk_path {
            self.put_single(key, data, BackendOperation::new_local(vdisk_id, path))
                .await
        } else {
            error!(
                "PUT[{}] dont now what to with data: op: {:?}. Data is not local and alien",
                key, options
            );
            Err(Error::Internal)
        };
        result.map_err(|e| e.convert_backend())
    }

    pub async fn put_local(
        &self,
        key: BobKey,
        data: BobData,
        operation: BackendOperation,
    ) -> PutResult {
        self.put_single(key, data, operation)
            .await
            .map_err(|e| e.convert_backend())
    }

    async fn put_single(
        &self,
        key: BobKey,
        data: BobData,
        operation: BackendOperation,
    ) -> PutResult {
        if !operation.is_data_alien() {
            debug!("PUT[{}] to backend: {}", key, operation);
            let result = self
                .backend
                .put(operation.clone(), key, data.clone())
                .0
                .boxed()
                .await;
            match result {
                Err(err) if err.is_put_error_need_alien() => {
                    error!(
                        "PUT[{}][{}] to backend. Error: {:?}",
                        key,
                        operation.disk_name_local(),
                        err
                    );
                    // write to alien/<local name>
                    let mut op = operation.clone_alien();
                    op.set_remote_folder(&self.mapper.local_node_name());
                    self.backend
                        .put_alien(op, key, data)
                        .0
                        .boxed()
                        .await
                        .map_err(|_| err) //we must return 'local' error if both ways are failed
                }
                _ => result,
            }
        } else {
            debug!("PUT[{}] to backend, alien data: {}", key, operation);
            self.backend.put_alien(operation, key, data).0.boxed().await
        }
    }

    pub async fn get(&self, key: BobKey, options: BobOptions) -> GetResult {
        let (vdisk_id, disk_path) = self.mapper.get_operation(key);

        // we cannot get data from alien if it belong this node
        let result = if options.get_normal() {
            if let Some(path) = disk_path.clone() {
                trace!("GET[{}] try read normal", key);
                self.get_local(key, BackendOperation::new_local(vdisk_id, path))
                    .await
            } else {
                error!(
                    "GET[{}] we must read data normaly but cannot find in config right path",
                    key
                );
                Err(Error::Internal)
            }
        }
        //TODO how read from all alien folders?
        else if options.get_alien() {
            //TODO check is alien? how? add field to grpc
            trace!("GET[{}] try read alien", key);
            //TODO read from all vdisk ids
            let mut op = BackendOperation::new_alien(vdisk_id.clone());
            op.set_remote_folder(&self.mapper.local_node_name());

            Self::get_single(self.backend.clone(), key, op).await
        } else {
            error!(
                "GET[{}] we cannot read data from anywhere. path: {:?}, options: {:?}",
                key, disk_path, options
            );
            Err(Error::Internal)
        };
        result.map_err(|e| e.convert_backend())
    }

    pub async fn get_local(&self, key: BobKey, op: BackendOperation) -> GetResult {
        Self::get_single(self.backend.clone(), key, op)
            .await
            .map_err(|e| e.convert_backend())
    }

    async fn get_single(
        backend: Arc<dyn BackendStorage + Send + Sync>,
        key: BobKey,
        operation: BackendOperation,
    ) -> GetResult {
        if !operation.is_data_alien() {
            debug!("GET[{}][{}] to backend", key, operation.disk_name_local());
            backend.get(operation, key).0.boxed().await
        } else {
            debug!("GET[{}] to backend, foreign data", key);
            backend.get_alien(operation, key).0.boxed().await
        }
    }
}
