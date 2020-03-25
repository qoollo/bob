use super::prelude::*;

#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct BackendOperation {
    vdisk_id: VDiskId,
    disk_path: Option<DiskPath>,
    remote_node_name: Option<String>, // save data to alien/<remote_node_name>
}

impl BackendOperation {
    pub(crate) fn vdisk_id(&self) -> VDiskId {
        self.vdisk_id
    }

    pub(crate) fn remote_node_name(&self) -> Option<&str> {
        self.remote_node_name.as_deref()
    }
}

impl Debug for BackendOperation {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        f.debug_struct("BackendOperation")
            .field("id", &self.vdisk_id)
            .field("path", &self.disk_path)
            .field("node", &self.remote_node_name)
            .field("alien", &self.is_data_alien())
            .finish()
    }
}

impl BackendOperation {
    pub(crate) fn new_alien(vdisk_id: VDiskId) -> Self {
        Self {
            vdisk_id,
            disk_path: None,
            remote_node_name: None,
        }
    }

    pub(crate) fn new_local(vdisk_id: VDiskId, path: DiskPath) -> Self {
        Self {
            vdisk_id,
            disk_path: Some(path),
            remote_node_name: None,
        }
    }

    pub(crate) fn clone_alien(&self) -> Self {
        Self {
            vdisk_id: self.vdisk_id,
            disk_path: None,
            remote_node_name: Some(self.remote_node_name.as_ref().unwrap().to_owned()),
        }
    }

    #[inline]
    pub(crate) fn set_remote_folder(&mut self, name: String) {
        self.remote_node_name = Some(name);
    }

    #[inline]
    pub(crate) fn is_data_alien(&self) -> bool {
        self.disk_path.is_none()
    }

    #[inline]
    pub(crate) fn disk_name_local(&self) -> String {
        self.disk_path.as_ref().expect("no path").name().to_owned()
    }
}

pub(crate) type GetResult = Result<BobData, Error>;
pub(crate) type Get = Pin<Box<dyn Future<Output = GetResult> + Send>>;
pub(crate) type PutResult = Result<(), Error>;
pub(crate) type Put = Pin<Box<dyn Future<Output = PutResult> + Send>>;
pub(crate) type RunResult = Result<(), Error>;
pub(crate) type Run = Pin<Box<dyn Future<Output = RunResult> + Send>>;
pub(crate) type ExistResult = Result<Vec<bool>, Error>;
pub(crate) type Exist = Pin<Box<dyn Future<Output = ExistResult> + Send>>;

pub(crate) trait BackendStorage: Debug {
    fn run_backend(&self) -> Run;

    fn put(&self, operation: BackendOperation, key: BobKey, data: BobData) -> Put;
    fn put_alien(
        &self,
        operation: BackendOperation,
        key: BobKey,
        data: BobData,
    ) -> Pin<Box<dyn Future<Output = PutResult> + Send>>;

    fn get(&self, operation: BackendOperation, key: BobKey) -> Get;
    fn get_alien(&self, operation: BackendOperation, key: BobKey) -> Get;

    fn exist(&self, operation: BackendOperation, keys: &[BobKey]) -> Exist;
    fn exist_alien(&self, operation: BackendOperation, keys: &[BobKey]) -> Exist;

    fn vdisks_groups(&self) -> Option<&[Group]> {
        None
    }
}

#[derive(Debug)]
pub(crate) struct Backend {
    inner: Arc<dyn BackendStorage + Send + Sync>,
    mapper: Arc<Virtual>,
}

impl Backend {
    pub(crate) fn new(mapper: Arc<Virtual>, config: &NodeConfig) -> Self {
        let inner: Arc<dyn BackendStorage + Send + Sync + 'static> = match config.backend_type() {
            BackendType::InMemory => Arc::new(MemBackend::new(&mapper)),
            BackendType::Stub => Arc::new(StubBackend {}),
            BackendType::Pearl => Arc::new(Pearl::new(mapper.clone(), config)),
        };
        Self { inner, mapper }
    }

    pub(crate) fn mapper(&self) -> &Virtual {
        &self.mapper
    }

    pub(crate) fn inner(&self) -> &dyn BackendStorage {
        self.inner.as_ref()
    }

    #[inline]
    pub(crate) async fn run_backend(&self) -> RunResult {
        self.inner.run_backend().await
    }

    pub(crate) async fn put(&self, key: BobKey, data: BobData, options: BobOptions) -> PutResult {
        let (vdisk_id, disk_path) = self.mapper.get_operation(key);
        if options.have_remote_node() {
            // write to all remote_nodes
            for node_name in options.remote_nodes() {
                let mut op = BackendOperation::new_alien(vdisk_id);
                op.set_remote_folder(node_name.to_owned());

                //TODO make it parallel?
                self.put_single(key, data.clone(), op).await?;
            }
            Ok(())
        } else if let Some(path) = disk_path {
            self.put_single(key, data, BackendOperation::new_local(vdisk_id, path))
                .await
        } else {
            error!(
                "PUT[{}] dont now what to do with data: op: {:?}. Data is not local and alien",
                key, options
            );
            Err(Error::Internal)
        }
    }

    #[inline]
    pub(crate) async fn put_local(
        &self,
        key: BobKey,
        data: BobData,
        operation: BackendOperation,
    ) -> PutResult {
        self.put_single(key, data, operation).await
    }

    async fn put_single(
        &self,
        key: BobKey,
        data: BobData,
        operation: BackendOperation,
    ) -> PutResult {
        if operation.is_data_alien() {
            debug!("PUT[{}] to backend, alien data: {:?}", key, operation);
            self.inner.put_alien(operation, key, data).await
        } else {
            debug!("PUT[{}] to backend: {:?}", key, operation);
            let result = self.inner.put(operation.clone(), key, data.clone()).await;
            match result {
                Err(err) if !err.is_duplicate() => {
                    error!(
                        "PUT[{}][{}] to backend. Error: {:?}",
                        key,
                        operation.disk_name_local(),
                        err
                    );
                    // write to alien/<local name>
                    let mut op = operation.clone_alien();
                    op.set_remote_folder(self.mapper.local_node_name().to_owned());
                    self.inner.put_alien(op, key, data).await.map_err(|_| err)
                    // @TODO return both errors| we must return 'local' error if both ways are failed
                }
                _ => result,
            }
        }
    }

    pub(crate) async fn get(&self, key: BobKey, options: &BobOptions) -> GetResult {
        let (vdisk_id, disk_path) = self.mapper.get_operation(key);

        // we cannot get data from alien if it belong this node

        if options.get_normal() {
            if let Some(path) = disk_path {
                trace!("GET[{}] try read normal", key);
                self.get_local(key, BackendOperation::new_local(vdisk_id, path.clone()))
                    .await
            } else {
                error!("GET[{}] we read data but can't find path in config", key);
                Err(Error::Internal)
            }
        }
        //TODO how read from all alien folders?
        else if options.get_alien() {
            //TODO check is alien? how? add field to grpc
            trace!("GET[{}] try read alien", key);
            //TODO read from all vdisk ids
            let op = BackendOperation::new_alien(vdisk_id);
            self.get_single(key, op).await
        } else {
            error!(
                "GET[{}] can't read from anywhere {:?}, {:?}",
                key, disk_path, options
            );
            Err(Error::Internal)
        }
    }

    pub(crate) async fn get_local(&self, key: BobKey, op: BackendOperation) -> GetResult {
        self.get_single(key, op).await
    }

    async fn get_single(&self, key: BobKey, operation: BackendOperation) -> GetResult {
        if operation.is_data_alien() {
            debug!("GET[{}] to backend, foreign data", key);
            self.inner.get_alien(operation, key).await
        } else {
            debug!("GET[{}][{}] to backend", key, operation.disk_name_local());
            self.inner.get(operation, key).await
        }
    }

    pub(crate) async fn exist(&self, keys: &[BobKey], options: &BobOptions) -> ExistResult {
        let mut exist = vec![false; keys.len()];
        let keys_by_id_and_path = self.group_keys_by_operations(keys, options);
        for (operation, (keys, indexes)) in keys_by_id_and_path {
            let result = self.inner.exist(operation, &keys).await;
            if let Ok(result) = result {
                for (&res, ind) in result.iter().zip(indexes) {
                    exist[ind] = res;
                }
            }
        }
        Ok(exist)
    }

    fn group_keys_by_operations(
        &self,
        keys: &[BobKey],
        options: &BobOptions,
    ) -> HashMap<BackendOperation, (Vec<BobKey>, Vec<usize>)> {
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
        if options.get_normal() {
            path.map(|path| BackendOperation::new_local(vdisk_id, path))
        } else if options.get_alien() {
            Some(BackendOperation::new_alien(vdisk_id))
        } else {
            None
        }
    }
}
