use crate::prelude::*;
use std::{
    collections::HashMap,
    fmt::{Display, Formatter, Result as FMTResult},
};

use crate::{
    mem_backend::MemBackend,
    pearl::{DiskController, Pearl},
    stub_backend::StubBackend,
    interval_logger::IntervalLoggerSafe
};
use log::Level;

pub const BACKEND_STARTING: f64 = 0f64;
pub const BACKEND_STARTED: f64 = 1f64;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Operation {
    vdisk_id: VDiskId,
    disk_path: Option<DiskPath>,
    remote_node_name: Option<String>, // save data to alien/<remote_node_name>
}

impl Operation {
    pub fn vdisk_id(&self) -> VDiskId {
        self.vdisk_id
    }

    pub fn remote_node_name(&self) -> Option<&str> {
        self.remote_node_name.as_deref()
    }
}

impl Debug for Operation {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        f.debug_struct("Operation")
            .field("vdisk_id", &self.vdisk_id)
            .field("path", &self.disk_path)
            .field("node", &self.remote_node_name)
            .field("alien", &self.is_data_alien())
            .finish()
    }
}

impl Operation {
    pub fn new_alien(vdisk_id: VDiskId) -> Self {
        Self {
            vdisk_id,
            disk_path: None,
            remote_node_name: None,
        }
    }

    pub fn new_local(vdisk_id: VDiskId, path: DiskPath) -> Self {
        Self {
            vdisk_id,
            disk_path: Some(path),
            remote_node_name: None,
        }
    }

    // local operation doesn't contain remote node, so node name is passed through argument
    pub fn clone_local_alien(&self, local_node_name: &str) -> Self {
        Self {
            vdisk_id: self.vdisk_id,
            disk_path: None,
            remote_node_name: Some(local_node_name.to_owned()),
        }
    }

    #[inline]
    pub fn set_remote_folder(&mut self, name: String) {
        self.remote_node_name = Some(name);
    }

    #[inline]
    pub fn is_data_alien(&self) -> bool {
        self.disk_path.is_none()
    }

    #[inline]
    pub fn disk_name_local(&self) -> String {
        self.disk_path.as_ref().expect("no path").name().to_owned()
    }
}

#[async_trait]
pub trait BackendStorage: Debug + MetricsProducer + Send + Sync + 'static {
    async fn run_backend(&self) -> AnyResult<()>;
    async fn run(&self) -> AnyResult<()> {
        gauge!(BACKEND_STATE, BACKEND_STARTING);
        let result = self.run_backend().await;
        gauge!(BACKEND_STATE, BACKEND_STARTED);
        result
    }

    async fn put(&self, op: Operation, key: BobKey, data: BobData) -> Result<(), Error>;
    async fn put_alien(&self, op: Operation, key: BobKey, data: BobData) -> Result<(), Error>;

    async fn get(&self, op: Operation, key: BobKey) -> Result<BobData, Error>;
    async fn get_alien(&self, op: Operation, key: BobKey) -> Result<BobData, Error>;

    async fn exist(&self, op: Operation, keys: &[BobKey]) -> Result<Vec<bool>, Error>;
    async fn exist_alien(&self, op: Operation, keys: &[BobKey]) -> Result<Vec<bool>, Error>;

    async fn shutdown(&self);

    // Should return pair: slice of normal disks and disk with aliens (because some method require
    // to work with only normal disk_controllers, we should return alien disk_controller explicitly
    // but not as part of other disk_controllers)
    fn disk_controllers(&self) -> Option<(&[Arc<DiskController>], Arc<DiskController>)> {
        None
    }

    async fn close_unneeded_active_blobs(&self, _soft: usize, _hard: usize) {}

    async fn offload_old_filters(&self, _limit: usize) {}

    async fn filter_memory_allocated(&self) -> usize {
        0
    }
}

#[async_trait]
pub trait MetricsProducer: Send + Sync {
    async fn blobs_count(&self) -> (usize, usize) {
        (0, 0)
    }

    async fn active_disks_count(&self) -> usize {
        0
    }

    async fn index_memory(&self) -> usize {
        0
    }
}

#[derive(Hash, PartialEq, Eq, Debug)]
enum BackendErrorAction {
    PUT(String, String),
}

impl Display for BackendErrorAction {
    fn fmt(&self, f: &mut Formatter<'_>) -> FMTResult {
        match self {
            BackendErrorAction::PUT(disk, error) => {
                write!(f, "local PUT on disk {} failed with error: {:?}", disk, error)
            },
        }
    }
}

impl BackendErrorAction {
    fn put(disk: String, error: &Error) -> Self {
        BackendErrorAction::PUT(disk, error.to_string())
    }
}

const ERROR_LOG_INTERVAL: u64 = 5000;

#[derive(Debug)]
pub struct Backend {
    inner: Arc<dyn BackendStorage>,
    mapper: Arc<Virtual>,
    error_logger: IntervalLoggerSafe<BackendErrorAction>,
}

impl Backend {
    pub async fn new(mapper: Arc<Virtual>, config: &NodeConfig) -> Self {
        let inner: Arc<dyn BackendStorage> = match config.backend_type() {
            BackendType::InMemory => Arc::new(MemBackend::new(&mapper)),
            BackendType::Stub => Arc::new(StubBackend {}),
            BackendType::Pearl => {
                let pearl = Pearl::new(mapper.clone(), config)
                    .await
                    .expect("pearl initialization failed");
                Arc::new(pearl)
            }
        };
        let error_logger = IntervalLoggerSafe::new(ERROR_LOG_INTERVAL, Level::Error);

        Self { inner, mapper, error_logger }
    }

    pub async fn blobs_count(&self) -> (usize, usize) {
        self.inner.blobs_count().await
    }

    pub async fn active_disks_count(&self) -> usize {
        self.inner.active_disks_count().await
    }

    pub async fn index_memory(&self) -> usize {
        self.inner.index_memory().await
    }

    pub fn mapper(&self) -> &Virtual {
        &self.mapper
    }

    pub fn inner(&self) -> &dyn BackendStorage {
        self.inner.as_ref()
    }

    #[inline]
    pub async fn run_backend(&self) -> AnyResult<()> {
        self.inner.run().await
    }

    pub async fn put(&self, key: BobKey, data: BobData, options: BobOptions) -> Result<(), Error> {
        trace!(">>>>>>- - - - - BACKEND PUT START - - - - -");
        let sw = Stopwatch::start_new();
        let (vdisk_id, disk_path) = self.mapper.get_operation(key);
        trace!(
            "get operation {:?}, /{:.3}ms/",
            disk_path,
            sw.elapsed().as_secs_f64() * 1000.0
        );
        let res = if !options.remote_nodes().is_empty() {
            // write to all remote_nodes
            for node_name in options.remote_nodes() {
                debug!("PUT[{}] core backend put remote node: {}", key, node_name);
                let mut op = Operation::new_alien(vdisk_id);
                op.set_remote_folder(node_name.to_owned());

                //TODO make it parallel?
                self.put_single(key, data.clone(), op).await?;
            }
            Ok(())
        } else if let Some(path) = disk_path {
            debug!(
                "remote nodes is empty, /{:.3}ms/",
                sw.elapsed().as_secs_f64() * 1000.0
            );
            let res = self
                .put_single(key, data, Operation::new_local(vdisk_id, path))
                .await;
            trace!("put single, /{:.3}ms/", sw.elapsed().as_secs_f64() * 1000.0);
            res
        } else {
            error!(
                "PUT[{}] dont now what to do with data: op: {:?}. Data is not local and alien",
                key, options
            );
            Err(Error::internal())
        };
        trace!("<<<<<<- - - - - BACKEND PUT FINISH - - - - -");
        res
    }

    #[inline]
    pub async fn put_local(
        &self,
        key: BobKey,
        data: BobData,
        operation: Operation,
    ) -> Result<(), Error> {
        self.put_single(key, data, operation).await
    }

    async fn put_single(
        &self,
        key: BobKey,
        data: BobData,
        operation: Operation,
    ) -> Result<(), Error> {
        if operation.is_data_alien() {
            debug!("PUT[{}] to backend, alien data: {:?}", key, operation);
            self.inner.put_alien(operation, key, data).await
        } else {
            debug!("PUT[{}] to backend: {:?}", key, operation);
            let result = self.inner.put(operation.clone(), key, data.clone()).await;
            match result {
                Err(local_err) if !local_err.is_duplicate() => {
                    debug!(
                        "PUT[{}][{}] local failed: {:?}",
                        key,
                        operation.disk_name_local(),
                        local_err
                    );
                    let error_to_log = BackendErrorAction::put(operation.disk_name_local(), &local_err);
                    self.error_logger.report_error(error_to_log);

                    // write to alien/<local name>
                    let mut op = operation.clone_local_alien(self.mapper().local_node_name());
                    op.set_remote_folder(self.mapper.local_node_name().to_owned());
                    self.inner
                        .put_alien(op, key, data)
                        .await
                        .map_err(|alien_err| {
                            Error::request_failed_completely(&local_err, &alien_err)
                        })
                    // @TODO return both errors| we must return 'local' error if both ways are failed
                }
                _ => result,
            }
        }
    }

    pub async fn get(&self, key: BobKey, options: &BobOptions) -> Result<BobData, Error> {
        let (vdisk_id, disk_path) = self.mapper.get_operation(key);

        // we cannot get data from alien if it belong this node

        if options.get_normal() {
            if let Some(path) = disk_path {
                trace!("GET[{}] try read normal", key);
                self.get_local(key, Operation::new_local(vdisk_id, path.clone()))
                    .await
            } else {
                error!("GET[{}] we read data but can't find path in config", key);
                Err(Error::internal())
            }
        }
        //TODO how read from all alien folders?
        else if options.get_alien() {
            //TODO check is alien? how? add field to grpc
            trace!("GET[{}] try read alien", key);
            //TODO read from all vdisk ids
            let op = Operation::new_alien(vdisk_id);
            self.get_single(key, op).await
        } else {
            error!(
                "GET[{}] can't read from anywhere {:?}, {:?}",
                key, disk_path, options
            );
            Err(Error::internal())
        }
    }

    pub async fn get_local(&self, key: BobKey, op: Operation) -> Result<BobData, Error> {
        self.get_single(key, op).await
    }

    async fn get_single(&self, key: BobKey, operation: Operation) -> Result<BobData, Error> {
        if operation.is_data_alien() {
            debug!("GET[{}] to backend, foreign data", key);
            self.inner.get_alien(operation, key).await
        } else {
            debug!("GET[{}][{}] to backend", key, operation.disk_name_local());
            self.inner.get(operation, key).await
        }
    }

    pub async fn exist(&self, keys: &[BobKey], options: &BobOptions) -> Result<Vec<bool>, Error> {
        let mut exist = vec![false; keys.len()];
        let keys_by_id_and_path = self.group_keys_by_operations(keys, options);
        for (operation, (keys, indexes)) in keys_by_id_and_path {
            let result = self.inner.exist(operation, &keys).await;
            if let Ok(result) = result {
                for (&res, ind) in result.iter().zip(indexes) {
                    exist[ind] |= res;
                }
            }
        }
        Ok(exist)
    }

    pub async fn shutdown(&self) {
        self.inner.shutdown().await;
    }

    fn group_keys_by_operations(
        &self,
        keys: &[BobKey],
        options: &BobOptions,
    ) -> HashMap<Operation, (Vec<BobKey>, Vec<usize>)> {
        let mut keys_by_operations: HashMap<_, (Vec<_>, Vec<_>)> = HashMap::new();
        for (ind, &key) in keys.iter().enumerate() {
            let operation = self.find_operation(key, options);
            if let Some(operation) = operation {
                keys_by_operations
                    .entry(operation)
                    .and_modify(|(keys, indexes)| {
                        keys.push(key);
                        indexes.push(ind);
                    })
                    .or_insert_with(|| (vec![key], vec![ind]));
            }
        }
        keys_by_operations
    }

    fn find_operation(&self, key: BobKey, options: &BobOptions) -> Option<Operation> {
        let (vdisk_id, path) = self.mapper.get_operation(key);
        if options.get_normal() {
            path.map(|path| Operation::new_local(vdisk_id, path))
        } else if options.get_alien() {
            Some(Operation::new_alien(vdisk_id))
        } else {
            None
        }
    }

    pub async fn close_unneeded_active_blobs(&self, soft: usize, hard: usize) {
        self.inner.close_unneeded_active_blobs(soft, hard).await
    }

    pub async fn offload_old_filters(&self, limit: usize) {
        self.inner.offload_old_filters(limit).await
    }

    pub async fn filter_memory_allocated(&self) -> usize {
        self.inner.filter_memory_allocated().await
    }
}
