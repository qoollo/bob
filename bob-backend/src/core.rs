const BACKEND_STARTING: i64 = 0;
const BACKEND_STARTED: i64 = 1;

#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct Operation {
    vdisk_id: VDiskID,
    disk_path: Option<DiskPath>,
    remote_node_name: Option<String>, // save data to alien/<remote_node_name>
}

impl Operation {
    pub(crate) fn vdisk_id(&self) -> VDiskID {
        self.vdisk_id
    }

    pub(crate) fn remote_node_name(&self) -> Option<&str> {
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
    pub(crate) fn new_alien(vdisk_id: VDiskID) -> Self {
        Self {
            vdisk_id,
            disk_path: None,
            remote_node_name: None,
        }
    }

    pub(crate) fn new_local(vdisk_id: VDiskID, path: DiskPath) -> Self {
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

#[async_trait]
pub(crate) trait BackendStorage: Debug {
    async fn run_backend(&self) -> Result<()>;

    async fn put(&self, op: Operation, key: BobKey, data: BobData) -> Result<(), Error>;
    async fn put_alien(&self, op: Operation, key: BobKey, data: BobData) -> Result<(), Error>;

    async fn get(&self, op: Operation, key: BobKey) -> Result<BobData, Error>;
    async fn get_alien(&self, op: Operation, key: BobKey) -> Result<BobData, Error>;

    async fn exist(&self, op: Operation, keys: &[BobKey]) -> Result<Vec<bool>, Error>;
    async fn exist_alien(&self, op: Operation, keys: &[BobKey]) -> Result<Vec<bool>, Error>;

    async fn run(&self) -> Result<()> {
        gauge!(BACKEND_STATE, BACKEND_STARTING);
        let result = self.run_backend().await;
        gauge!(BACKEND_STATE, BACKEND_STARTED);
        result
    }

    async fn blobs_count(&self) -> (usize, usize) {
        (0, 0)
    }

    async fn index_memory(&self) -> usize {
        0
    }

    async fn shutdown(&self);

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

    pub(crate) async fn blobs_count(&self) -> (usize, usize) {
        self.inner.blobs_count().await
    }

    pub(crate) async fn index_memory(&self) -> usize {
        self.inner.index_memory().await
    }

    pub(crate) fn mapper(&self) -> &Virtual {
        &self.mapper
    }

    pub(crate) fn inner(&self) -> &dyn BackendStorage {
        self.inner.as_ref()
    }

    #[inline]
    pub(crate) async fn run_backend(&self) -> Result<()> {
        self.inner.run().await
    }

    pub(crate) async fn put(
        &self,
        key: BobKey,
        data: BobData,
        options: BobOptions,
    ) -> Result<(), Error> {
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
    pub(crate) async fn put_local(
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
                    error!(
                        "PUT[{}][{}] local failed: {:?}",
                        key,
                        operation.disk_name_local(),
                        local_err
                    );
                    // write to alien/<local name>
                    let mut op = operation.clone_alien();
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

    pub(crate) async fn get(&self, key: BobKey, options: &BobOptions) -> Result<BobData, Error> {
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

    pub(crate) async fn get_local(&self, key: BobKey, op: Operation) -> Result<BobData, Error> {
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

    pub(crate) async fn exist(
        &self,
        keys: &[BobKey],
        options: &BobOptions,
    ) -> Result<Vec<bool>, Error> {
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

    pub(crate) async fn close_unneeded_active_blobs(&self, soft: usize, hard: usize) {
        let groups = if let Some(groups) = self.inner.vdisks_groups() {
            groups
        } else {
            return;
        };
        for group in groups {
            let holders_lock = group.holders();
            let mut holders_write = holders_lock.write().await;
            let holders: &mut Vec<_> = holders_write.as_mut();

            let mut total_open_blobs = 0;
            let mut close = vec![];
            for h in holders.iter_mut() {
                if !h.active_blob_is_empty().await {
                    total_open_blobs += 1;
                    if h.is_outdated() && h.no_writes_recently().await {
                        close.push(h);
                    }
                }
            }
            let soft = soft.saturating_sub(total_open_blobs - close.len());
            let hard = hard.saturating_sub(total_open_blobs - close.len());

            debug!(
                "closing outdated blobs according to limits ({}, {})",
                soft, hard
            );

            let mut is_small = vec![];
            for h in &close {
                is_small.push(h.active_blob_is_small().await);
            }

            let mut close: Vec<_> = close.into_iter().enumerate().collect();
            Self::sort_by_priority(&mut close, &is_small);

            while close.len() > hard {
                let (_, holder) = close.pop().unwrap();
                holder.close_active_blob().await;
                info!("active blob of {} closed by hard cap", holder.get_id());
            }

            while close.len() > soft && close.last().map_or(false, |(ind, _)| !is_small[*ind]) {
                let (_, holder) = close.pop().unwrap();
                holder.close_active_blob().await;
                info!("active blob of {} closed by soft cap", holder.get_id());
            }
        }
    }

    fn sort_by_priority(close: &mut [(usize, &mut Holder)], is_small: &[bool]) {
        use std::cmp::Ordering;
        close.sort_by(|(i, x), (j, y)| match (is_small[*i], is_small[*j]) {
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            _ => x.end_timestamp().cmp(&y.end_timestamp()),
        });
    }
}
