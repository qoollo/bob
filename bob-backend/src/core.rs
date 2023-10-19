use crate::prelude::*;
use std::{
    collections::HashMap,
    fmt::{Display, Formatter, Result as FMTResult},
    hash::Hash,
};
use smallvec::SmallVec;

use crate::{
    mem_backend::MemBackend,
    pearl::{DiskController, Pearl},
    stub_backend::StubBackend,
};
use log::Level;

use bob_common::{interval_logger::IntervalLoggerSafe, metrics::BLOOM_FILTERS_RAM};

pub const BACKEND_STARTING: f64 = 0f64;
pub const BACKEND_STARTED: f64 = 1f64;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Operation {
    vdisk_id: VDiskId,
    disk_path: Option<DiskPath>,
    remote_node_name: Option<NodeName>, // save data to alien/<remote_node_name>
}

impl Operation {
    pub fn vdisk_id(&self) -> VDiskId {
        self.vdisk_id
    }

    pub fn remote_node_name(&self) -> Option<&NodeName> {
        self.remote_node_name.as_ref()
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

    #[inline]
    pub fn set_remote_node_name(&mut self, name: NodeName) {
        self.remote_node_name = Some(name);
    }

    #[inline]
    pub fn is_data_alien(&self) -> bool {
        self.disk_path.is_none()
    }

    #[inline]
    pub fn disk_name_local(&self) -> &DiskName {
        self.disk_path.as_ref().expect("no path").name()
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

    async fn put(&self, op: Operation, key: BobKey, data: &BobData) -> Result<(), Error>;
    async fn put_alien(&self, op: Operation, key: BobKey, data: &BobData) -> Result<(), Error>;

    async fn get(&self, op: Operation, key: BobKey) -> Result<BobData, Error>;
    async fn get_alien(&self, op: Operation, key: BobKey) -> Result<BobData, Error>;

    async fn exist(&self, op: Operation, keys: &[BobKey]) -> Result<Vec<bool>, Error>;
    async fn exist_alien(&self, op: Operation, keys: &[BobKey]) -> Result<Vec<bool>, Error>;

    async fn delete(&self, op: Operation, key: BobKey, meta: &BobMeta) -> Result<u64, Error>;
    async fn delete_alien(&self, op: Operation, key: BobKey, meta: &BobMeta, force_delete: bool) -> Result<u64, Error>;

    async fn shutdown(&self);

    // Should return pair: slice of normal disks and disk with aliens (because some method require
    // to work with only normal disk_controllers, we should return alien disk_controller explicitly
    // but not as part of other disk_controllers)
    fn disk_controllers(&self) -> Option<(&[Arc<DiskController>], Arc<DiskController>)> {
        None
    }

    async fn close_unneeded_active_blobs(&self, _soft: usize, _hard: usize) {}
    async fn close_oldest_active_blob(&self) -> Option<usize> {
        None
    }
    async fn free_least_used_resources(&self) -> Option<usize> {
        None
    }

    async fn offload_old_filters(&self, _limit: usize) {}

    async fn filter_memory_allocated(&self) -> usize {
        0
    }

    async fn remount_vdisk(&self, _vdisk_id: u32) -> AnyResult<()> {
        Ok(())
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

    async fn disk_used_by_disk(&self) -> HashMap<DiskPath, u64> {
        HashMap::new()
    }

    async fn corrupted_blobs_count(&self) -> usize {
        0
    }
}

#[derive(Hash, PartialEq, Eq, Debug)]
enum BackendErrorAction {
    PUT(DiskName, String),
}

impl Display for BackendErrorAction {
    fn fmt(&self, f: &mut Formatter<'_>) -> FMTResult {
        match self {
            BackendErrorAction::PUT(disk, error) => {
                write!(
                    f,
                    "local PUT on disk {} failed with error: {:?}",
                    disk, error
                )
            }
        }
    }
}

impl BackendErrorAction {
    fn put(disk: DiskName, error: &Error) -> Self {
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

        Self {
            inner,
            mapper,
            error_logger,
        }
    }

    pub async fn blobs_count(&self) -> (usize, usize) {
        self.inner.blobs_count().await
    }

    pub async fn corrupted_blobs_count(&self) -> usize {
        self.inner.corrupted_blobs_count().await
    }

    pub async fn active_disks_count(&self) -> usize {
        self.inner.active_disks_count().await
    }

    pub async fn index_memory(&self) -> usize {
        self.inner.index_memory().await
    }

    pub async fn disk_used_by_disk(&self) -> HashMap<DiskPath, u64> {
        self.inner.disk_used_by_disk().await
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

    pub async fn put(&self, key: BobKey, data: &BobData, options: BobPutOptions) -> Result<(), Error> {
        trace!(">>>>>>- - - - - BACKEND PUT START - - - - -");
        let sw = Stopwatch::start_new();
        let (vdisk_id, disk_paths) = self.mapper.get_operation(key);
        trace!(
            "get operation {:?}, /{:.3}ms/",
            disk_paths,
            sw.elapsed().as_secs_f64() * 1000.0
        );
        let res = if options.to_alien() {
            // write to all remote_nodes
            for node_name in options.remote_nodes() {
                debug!("PUT[{}] core backend put remote node: {}", key, node_name);
                let mut op = Operation::new_alien(vdisk_id);
                op.set_remote_node_name(node_name.clone());

                //TODO make it parallel?
                self.put_single(key, data, op).await?;
            }
            Ok(())
        } else if let Some(paths) = disk_paths {
            debug!(
                "remote nodes is empty, /{:.3}ms/",
                sw.elapsed().as_secs_f64() * 1000.0
            );
            let mut result = Ok(());
            for path in paths {
                if let Err(e) = self.put_single(key, data, Operation::new_local(vdisk_id, path.clone())).await {
                    warn!("PUT[{}] error put to {:?}: {:?}", key, path, e);
                    result = Err(e);
                }
            }
            trace!("put single, /{:.3}ms/", sw.elapsed().as_secs_f64() * 1000.0);
            result
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
        data: &BobData,
        operation: Operation,
    ) -> Result<(), Error> {
        self.put_single(key, data, operation).await
    }

    async fn update_bloom_filter_metrics(&self) {
        let bfr = self.filter_memory_allocated().await;
        gauge!(BLOOM_FILTERS_RAM, bfr as f64);
    }

    async fn put_single(
        &self,
        key: BobKey,
        data: &BobData,
        operation: Operation,
    ) -> Result<(), Error> {
        if operation.is_data_alien() {
            debug!("PUT[{}] to backend, alien data: {:?}", key, operation);
            self.inner.put_alien(operation, key, data).await
        } else {
            debug!("PUT[{}] to backend: {:?}", key, operation);
            let result = self.inner.put(operation.clone(), key, data).await;
            match result {
                Err(local_err) if !local_err.is_duplicate() => {
                    debug!(
                        "PUT[{}][{}] local failed: {:?}",
                        key,
                        operation.disk_name_local(),
                        local_err
                    );
                    let error_to_log =
                        BackendErrorAction::put(operation.disk_name_local().clone(), &local_err);
                    self.error_logger.report_error(error_to_log);

                    // write to alien/<local name>
                    let mut op = operation.clone();
                    op.set_remote_node_name(self.mapper.local_node_name().clone());
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

    pub async fn get(&self, key: BobKey, options: &BobGetOptions) -> Result<BobData, Error> {
        let (vdisk_id, disk_paths) = self.mapper.get_operation(key);

        // Get all first: we search both in local data and in aliens
        if options.get_all() {
            let mut all_result = Err(Error::key_not_found(key));
            if let Some(paths) = disk_paths {
                trace!("GET[{}] try read normal", key);
                for path in paths {
                    let result = self.get_local(key, Operation::new_local(vdisk_id, path)).await;
                    match result {
                        Ok(data) => return Ok(data),
                        Err(_) => continue,
                    }
                }
            }
            if all_result.is_err() {
                trace!("GET[{}] try read alien", key);
                all_result = self.get_single(key, Operation::new_alien(vdisk_id)).await;
            }
            all_result
        } 
        else if options.get_normal() {
            // Lookup local data
            if let Some(paths) = disk_paths {
                trace!("GET[{}] try read normal", key);
                let mut error = None;
                for path in paths {
                    match self.get_local(key, Operation::new_local(vdisk_id, path)).await {
                        Ok(data) => return Ok(data),
                        Err(e) => error = Some(e),
                    }
                }
                Err(error.unwrap_or(Error::key_not_found(key)))
            } else {
                error!("GET[{}] we read data but can't find path in config", key);
                Err(Error::internal())
            }
        }
        else if options.get_alien() {
            // Lookup aliens
            trace!("GET[{}] try read alien", key);
            self.get_single(key, Operation::new_alien(vdisk_id)).await
        } else {
            error!("GET[{}] can't read from anywhere {:?}, {:?}", key, disk_paths, options);
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

    pub async fn exist(&self, keys: &[BobKey], options: &BobGetOptions) -> Result<Vec<bool>, Error> {
        let mut exist = vec![false; keys.len()];
        let keys_by_id_and_path = self.group_keys_by_operations(keys, options);
        for (operation, (keys, indexes)) in keys_by_id_and_path {
            let result = if operation.is_data_alien() {
                self.inner.exist_alien(operation, &keys).await
            } else {
                self.inner.exist(operation, &keys).await
            };
            if let Ok(result) = result {
                for (&res, ind) in result.iter().zip(indexes) {
                    exist[ind] |= res;
                }
            }
        }
        Ok(exist)
    }

    fn group_keys_by_operations(
        &self,
        keys: &[BobKey],
        options: &BobGetOptions,
    ) -> HashMap<Operation, (Vec<BobKey>, Vec<usize>)> {
        let mut keys_by_operations: HashMap<_, (Vec<_>, Vec<_>)> = HashMap::new();
        for (ind, &key) in keys.iter().enumerate() {
            let operations = self.find_operations(key, options);
            for operation in operations {
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

    fn find_operations(&self, key: BobKey, options: &BobGetOptions) -> SmallVec<[Operation; 1]> {
        let (vdisk_id, paths) = self.mapper.get_operation(key);

        // With GET_ALL we should lookup both local data and aliens
        let capacity = if options.get_normal() { paths.as_ref().map(|v| v.len()).unwrap_or_default() } else { 0 } +
                              if options.get_alien() { 1 } else { 0 };

        let mut result = SmallVec::with_capacity(capacity);
        
        if options.get_normal() {
            if let Some(paths) = paths {
                for path in paths {
                    result.push(Operation::new_local(vdisk_id, path));
                }
            }
        } 
        if options.get_alien() {
            result.push(Operation::new_alien(vdisk_id));
        } 

        result
    }

    pub async fn delete(
        &self,
        key: BobKey,
        meta: &BobMeta,
        options: BobDeleteOptions
    ) -> Result<(), Error> {
        let (vdisk_id, disk_paths) = self.mapper.get_operation(key);
        if options.to_alien() {
            // Process all nodes for key
            let mut errors = Vec::new();
            for node in self.mapper.get_target_nodes_for_key(key) {
                let force_delete = options.is_force_delete(node.name());
                let mut op = Operation::new_alien(vdisk_id);
                op.set_remote_node_name(node.name().clone());
                let delete_res = self.delete_single(key, meta, op, force_delete).await;
                if let Err(err) = delete_res {
                    error!("DELETE[{}] Error deleting from aliens (node: {}, force_delete: {}): {:?}", key, node.name(), force_delete, err);
                    errors.push(err);
                }
            }
            if errors.len() == 0 {
                Ok(())
            } else if errors.len() == 1 {
                Err(errors.remove(0))
            } else {
                Err(Error::failed("Multiple errors detected"))
            }
        } else if let Some(paths) = disk_paths {
            let mut result = Ok(());
            for path in paths {
                let op = Operation::new_local(vdisk_id, path.clone());
                if let Err(e) = self.delete_single(key, meta, op, true).await.map(|_| ()) {
                    warn!("DELETE[{}] failed on path {:?}: {:?}", key, path, e);
                    result = Err(e);
                }
            }
            result
        } else {
            error!(
                "DELETE[{}] dont know what to do with data: op: {:?}. Data is not local and alien",
                key, options
            );
            Err(Error::internal())
        }
    }

    pub async fn delete_local(
        &self,
        key: BobKey,
        meta: &BobMeta,
        operation: Operation,
        force_delete: bool,
    ) -> Result<u64, Error> {
        self.delete_single(key, meta, operation, force_delete).await
    }

    async fn delete_single(
        &self,
        key: BobKey,
        meta: &BobMeta,
        operation: Operation,
        force_delete: bool,
    ) -> Result<u64, Error> {
        if operation.is_data_alien() {
            debug!("DELETE[{}] from backend, foreign data", key);
            self.inner.delete_alien(operation, key, meta, force_delete).await
        } else {
            debug!(
                "DELETE[{}][{}] from backend",
                key,
                operation.disk_name_local()
            );
            self.inner.delete(operation, key, meta).await
        }
    }

    pub async fn shutdown(&self) {
        self.inner.shutdown().await;
    }

    pub async fn close_unneeded_active_blobs(&self, soft: usize, hard: usize) {
        self.inner.close_unneeded_active_blobs(soft, hard).await
    }

    pub async fn close_oldest_active_blob(&self) -> Option<usize> {
        self.inner.close_oldest_active_blob().await
    }

    pub async fn free_least_used_holder_resources(&self) -> Option<usize> {
        self.inner.free_least_used_resources().await
    }

    pub async fn offload_old_filters(&self, limit: usize) {
        self.inner.offload_old_filters(limit).await;

        self.update_bloom_filter_metrics().await;
    }

    pub async fn filter_memory_allocated(&self) -> usize {
        self.inner.filter_memory_allocated().await
    }
}
