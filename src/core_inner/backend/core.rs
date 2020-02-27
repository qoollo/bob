use super::prelude::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BackendOperation {
    pub vdisk_id: VDiskId,
    disk_path: Option<DiskPath>,
    pub remote_node_name: Option<String>, // save data to alien/<remote_node_name>
}

impl std::fmt::Display for BackendOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "[id: {}, path: {:?}, node: {:?} alien: {}]",
            self.vdisk_id,
            self.disk_path,
            self.remote_node_name,
            self.is_data_alien()
        )
    }
}

impl BackendOperation {
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

    pub fn clone_alien(&self) -> Self {
        Self {
            vdisk_id: self.vdisk_id.clone(),
            disk_path: None,
            remote_node_name: self.remote_node_name.clone(),
        }
    }

    #[inline]
    pub fn set_remote_folder(&mut self, name: &str) {
        self.remote_node_name = Some(name.to_string())
    }

    #[inline]
    pub fn is_data_alien(&self) -> bool {
        self.disk_path.is_none()
    }

    #[inline]
    pub fn disk_name_local(&self) -> String {
        self.disk_path.clone().expect("disk path not set").name
    }

    #[inline]
    pub fn remote_node_name(&self) -> String {
        self.remote_node_name
            .clone()
            .expect("remote node name not set")
    }
}

#[derive(Debug, Clone)]
pub struct BackendPutResult;

#[derive(Debug, Clone)]
pub struct BackendGetResult {
    pub data: BobData,
}

#[derive(Debug, Clone)]
pub struct BackendExistResult {
    pub exist: Vec<bool>,
}

impl Display for BackendGetResult {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug)]
pub struct BackendPingResult;

pub type GetResult = Result<BackendGetResult, Error>;
pub struct Get(pub Pin<Box<dyn Future<Output = GetResult> + Send>>);

pub type PutResult = Result<BackendPutResult, Error>;
pub struct Put(pub Pin<Box<dyn Future<Output = PutResult> + Send>>);

pub type RunResult = Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>;

pub type ExistResult = Result<BackendExistResult, Error>;
pub struct Exist(pub Pin<Box<dyn Future<Output = ExistResult>>>);

pub(crate) trait BackendStorage: Debug {
    fn run_backend(&self) -> RunResult;

    fn put(&self, operation: BackendOperation, key: BobKey, data: BobData) -> Put;
    fn put_alien(&self, operation: BackendOperation, key: BobKey, data: BobData) -> Put;

    fn get(&self, operation: BackendOperation, key: BobKey) -> Get;
    fn get_alien(&self, operation: BackendOperation, key: BobKey) -> Get;

    fn exist(&self, operation: BackendOperation, keys: &[BobKey]) -> Exist;
    fn exist_alien(&self, operation: BackendOperation, keys: &[BobKey]) -> Exist;

    fn vdisks_groups(&self) -> Option<&[PearlGroup]> {
        None
    }
}

#[derive(Debug)]
pub struct Backend {
    backend: Arc<dyn BackendStorage + Send + Sync>,
    mapper: Arc<VDiskMapper>,
}

impl Backend {
    pub fn new(mapper: Arc<VDiskMapper>, config: &NodeConfig) -> Self {
        let backend: Arc<dyn BackendStorage + Send + Sync + 'static> = match config.backend_type() {
            BackendType::InMemory => Arc::new(MemBackend::new(&mapper)),
            BackendType::Stub => Arc::new(StubBackend {}),
            BackendType::Pearl => Arc::new(PearlBackend::new(mapper.clone(), config)),
        };
        Self { backend, mapper }
    }

    #[inline]
    pub async fn run_backend(&self) -> Result<(), Error> {
        self.backend.run_backend().await
    }

    pub async fn put(&self, key: BobKey, data: BobData, options: BobOptions) -> PutResult {
        let (vdisk_id, disk_path) = self.mapper.get_operation(key);
        if options.have_remote_node() {
            // write to all remote_nodes
            for node_name in options.remote_nodes {
                let mut op = BackendOperation::new_alien(vdisk_id.clone());
                op.set_remote_folder(&node_name);

                //TODO make it parallel?
                self.put_single(key, data.clone(), op).await?;
            }
        } else if let Some(path) = disk_path {
            self.put_single(key, data, BackendOperation::new_local(vdisk_id, path))
                .await?;
        } else {
            error!(
                "PUT[{}] dont now what to with data: op: {:?}. Data is not local and alien",
                key, options
            );
            return Err(Error::Internal);
        }
        Ok(BackendPutResult {})
    }

    #[inline]
    pub async fn put_local(
        &self,
        key: BobKey,
        data: BobData,
        operation: BackendOperation,
    ) -> PutResult {
        self.put_single(key, data, operation)
            .await
            .map_err(Error::convert_backend)
    }

    async fn put_single(
        &self,
        key: BobKey,
        data: BobData,
        operation: BackendOperation,
    ) -> PutResult {
        if operation.is_data_alien() {
            debug!("PUT[{}] to backend, alien data: {}", key, operation);
            self.backend.put_alien(operation, key, data).0.await
        } else {
            debug!("PUT[{}] to backend: {}", key, operation);
            let result = self
                .backend
                .put(operation.clone(), key, data.clone())
                .0
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
                        .await
                        .map_err(|_| err) //we must return 'local' error if both ways are failed
                }
                _ => result,
            }
        }
    }

    pub async fn get(&self, key: BobKey, options: &BobOptions) -> GetResult {
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
            let op = BackendOperation::new_alien(vdisk_id.clone());
            // op.set_remote_folder(&self.mapper.local_node_name());

            Self::get_single(self.backend.clone(), key, op).await
        } else {
            error!(
                "GET[{}] we cannot read data from anywhere. path: {:?}, options: {:?}",
                key, disk_path, options
            );
            Err(Error::Internal)
        };
        result.map_err(Error::convert_backend)
    }

    pub async fn get_local(&self, key: BobKey, op: BackendOperation) -> GetResult {
        Self::get_single(self.backend.clone(), key, op)
            .await
            .map_err(Error::convert_backend)
    }

    async fn get_single(
        backend: Arc<dyn BackendStorage + Send + Sync>,
        key: BobKey,
        operation: BackendOperation,
    ) -> GetResult {
        if operation.is_data_alien() {
            debug!("GET[{}] to backend, foreign data", key);
            backend.get_alien(operation, key).0.await
        } else {
            debug!("GET[{}][{}] to backend", key, operation.disk_name_local());
            backend.get(operation, key).0.await
        }
    }

    pub async fn exist(&self, keys: &[BobKey], options: &BobOptions) -> ExistResult {
        let mut exist = vec![false; keys.len()];
        let keys_by_id_and_path = self.group_keys_by_operations(keys, options);
        for (operation, (keys, indexes)) in keys_by_id_and_path {
            let result = self.backend.exist(operation, &keys).0.await;
            if let Ok(result) = result {
                for (&res, ind) in result.exist.iter().zip(indexes) {
                    exist[ind] = res;
                }
            }
        }
        Ok(BackendExistResult { exist })
    }

    fn group_keys_by_operations(
        &self,
        keys: &[BobKey],
        options: &BobOptions,
    ) -> HashMap<BackendOperation, (Vec<BobKey>, Vec<usize>), std::collections::hash_map::RandomState>
    {
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

    fn find_operation(&self, key: BobKey, options: &BobOptions) -> Option<BackendOperation> {
        let (vdisk_id, path) = self.mapper.get_operation(key);
        let operation = if options.get_normal() && path.is_some() {
            Some(BackendOperation::new_local(vdisk_id, path.unwrap()))
        } else if options.get_alien() {
            Some(BackendOperation::new_alien(vdisk_id))
        } else {
            None
        };
        operation
    }

    pub fn mapper(&self) -> &VDiskMapper {
        &self.mapper
    }

    pub(crate) fn backend(&self) -> &dyn BackendStorage {
        self.backend.as_ref()
    }
}
