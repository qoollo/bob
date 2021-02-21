use super::prelude::*;

pub(crate) type BackendResult<T> = std::result::Result<T, Error>;
pub(crate) type PearlStorage = Storage<Key>;

#[derive(Clone, Debug)]
pub(crate) struct Pearl {
    settings: Arc<Settings>,
    disk_controllers: Arc<[Arc<DiskController>]>,
    alien_disk_controller: Arc<DiskController>,
    node_name: String,
    init_par_degree: usize,
}

impl Pearl {
    pub(crate) fn new(mapper: Arc<Virtual>, config: &NodeConfig) -> Self {
        debug!("initializing pearl backend");
        let settings = Arc::new(Settings::new(config, mapper));

        let data = settings.clone().read_group_from_disk(config);
        let disk_controllers: Arc<[Arc<DiskController>]> = Arc::from(data.as_slice());
        trace!("count vdisk groups: {}", disk_controllers.len());

        let alien_disk_controller = settings
            .clone()
            .read_alien_directory(config)
            .expect("vec of pearl groups");

        Self {
            settings,
            disk_controllers,
            alien_disk_controller,
            node_name: config.name().to_string(),
            init_par_degree: config.init_par_degree(),
        }
    }
}

#[async_trait]
impl BackendStorage for Pearl {
    async fn run_backend(&self) -> Result<()> {
        use futures::stream::futures_unordered::FuturesUnordered;
        use std::iter::once;

        // vec![vec![]; n] macro requires Clone trait, and future does not implement it
        let start = Instant::now();
        let mut par_buckets: Vec<Vec<_>> = (0..self.init_par_degree).map(|_| vec![]).collect();
        for (ind, dc) in self
            .disk_controllers
            .iter()
            .chain(once(&self.alien_disk_controller))
            .cloned()
            .enumerate()
        {
            let buck = ind % self.init_par_degree;
            par_buckets[buck].push(async move { dc.run().await });
        }
        let futs = FuturesUnordered::new();
        for futures in par_buckets {
            futs.push(async move {
                for f in futures {
                    f.await?;
                }
                Ok::<(), anyhow::Error>(())
            });
        }
        futs.fold(Ok(()), |s, n| async move { s.and(n) }).await?;
        let dur = std::time::Instant::now() - start;
        debug!("pearl backend init took {:?}", dur);
        Ok(())
    }

    async fn put(&self, op: Operation, key: BobKey, data: BobData) -> Result<(), Error> {
        debug!("PUT[{}] to pearl backend. operation: {:?}", key, op);
        let dc_option = self
            .disk_controllers
            .iter()
            .find(|dc| dc.can_process_operation(&op));
        if let Some(disk_controller) = dc_option {
            disk_controller
                .put(op, key, data)
                .await
                .map_err(|e| Error::failed(format!("{:#?}", e)))
        } else {
            debug!(
                "PUT[{}] Cannot find disk_controller, operation: {:?}",
                key, op
            );
            Err(Error::dc_is_not_available())
        }
    }

    async fn put_alien(&self, op: Operation, key: BobKey, data: BobData) -> Result<(), Error> {
        debug!("PUT[alien][{}] to pearl backend, operation: {:?}", key, op);
        self.alien_disk_controller.put_alien(op, key, data).await
    }

    async fn get(&self, op: Operation, key: BobKey) -> Result<BobData, Error> {
        debug!("Get[{}] from pearl backend. operation: {:?}", key, op);
        let dc_option = self
            .disk_controllers
            .iter()
            .find(|dc| dc.can_process_operation(&op));

        if let Some(disk_controller) = dc_option {
            disk_controller.get(op, key).await
        } else {
            Err(Error::dc_is_not_available())
        }
    }

    async fn get_alien(&self, op: Operation, key: BobKey) -> Result<BobData, Error> {
        debug!("Get[alien][{}] from pearl backend", key);
        if self.alien_disk_controller.can_process_operation(&op) {
            self.alien_disk_controller.get_alien(op, key).await
        } else {
            Err(Error::dc_is_not_available())
        }
    }

    async fn exist(&self, operation: Operation, keys: &[BobKey]) -> Result<Vec<bool>, Error> {
        let dc_option = self
            .disk_controllers
            .iter()
            .find(|dc| dc.can_process_operation(&operation));
        if let Some(disk_controller) = dc_option {
            disk_controller.exist(operation, &keys).await
        } else {
            Err(Error::dc_is_not_available())
        }
    }

    async fn exist_alien(&self, operation: Operation, keys: &[BobKey]) -> Result<Vec<bool>, Error> {
        if self.alien_disk_controller.can_process_operation(&operation) {
            self.alien_disk_controller.exist(operation, &keys).await
        } else {
            Err(Error::dc_is_not_available())
        }
    }

    async fn shutdown(&self) {
        use futures::stream::FuturesUnordered;
        info!("begin shutdown");
        let futures = FuturesUnordered::new();
        for dc in self.disk_controllers.iter() {
            futures.push(async move { dc.shutdown().await })
        }
        let _ = futures.collect::<()>().await;
        // TODO: process alien disk with other ones
        self.alien_disk_controller.shutdown().await;
        info!("shutting down done");
    }

    async fn blobs_count(&self) -> (usize, usize) {
        let mut cnt = 0;
        for dc in self.disk_controllers.iter() {
            cnt += dc.blobs_count().await
        }
        let alien_cnt = self.alien_disk_controller.blobs_count().await;
        (cnt, alien_cnt)
    }

    async fn index_memory(&self) -> usize {
        let mut cnt = 0;
        for dc in self.disk_controllers.iter() {
            cnt += dc.index_memory().await
        }
        cnt += self.alien_disk_controller.index_memory().await;
        cnt
    }
}
