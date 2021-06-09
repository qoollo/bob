use futures::future::ready;
use std::iter::once;

use crate::prelude::*;

use super::{
    data::Key, disk_controller::logger::DisksEventsLogger, disk_controller::DiskController,
    settings::Settings,
};
use crate::core::{BackendStorage, MetricsProducer, Operation};

pub type BackendResult<T> = std::result::Result<T, Error>;
pub type PearlStorage = Storage<Key>;

#[derive(Clone, Debug)]
pub struct Pearl {
    settings: Arc<Settings>,
    disk_controllers: Arc<[Arc<DiskController>]>,
    alien_disk_controller: Arc<DiskController>,
    node_name: String,
    init_par_degree: usize,
}

impl Pearl {
    pub async fn new(mapper: Arc<Virtual>, config: &NodeConfig) -> Self {
        debug!("initializing pearl backend");
        let settings = Arc::new(Settings::new(config, mapper));
        let logfile = config.pearl().disks_events_logfile();
        let logger = DisksEventsLogger::new(logfile)
            .await
            .expect("create logger");

        let run_sem = Arc::new(Semaphore::new(config.init_par_degree()));
        let data = settings
            .clone()
            .read_group_from_disk(config, run_sem.clone(), logger.clone())
            .await;
        let disk_controllers: Arc<[_]> = Arc::from(data.as_slice());
        trace!("count vdisk groups: {}", disk_controllers.len());

        let alien_disk_controller = settings
            .clone()
            .read_alien_directory(config, run_sem, logger)
            .await;

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
impl MetricsProducer for Pearl {
    async fn blobs_count(&self) -> (usize, usize) {
        let futs: FuturesUnordered<_> = self
            .disk_controllers
            .iter()
            .cloned()
            .map(|dc| async move { dc.blobs_count().await })
            .collect();
        let cnt = futs.fold(0, |cnt, dc_cnt| ready(cnt + dc_cnt)).await;
        let alien_cnt = self.alien_disk_controller.blobs_count().await;
        (cnt, alien_cnt)
    }

    async fn index_memory(&self) -> usize {
        let futs: FuturesUnordered<_> = self
            .disk_controllers
            .iter()
            .chain(once(&self.alien_disk_controller))
            .cloned()
            .map(|dc| async move { dc.index_memory().await })
            .collect();
        let cnt = futs.fold(0, |cnt, dc_cnt| ready(cnt + dc_cnt)).await;
        cnt
    }

    async fn active_disks_count(&self) -> usize {
        let mut cnt = 0;
        for dc in self.disk_controllers.iter() {
            cnt += dc.is_ready().await as usize;
        }
        cnt
    }
}

#[async_trait]
impl BackendStorage for Pearl {
    async fn run_backend(&self) -> AnyResult<()> {
        // vec![vec![]; n] macro requires Clone trait, and future does not implement it
        let start = Instant::now();
        let futs = FuturesUnordered::new();
        let alien_iter = once(&self.alien_disk_controller);
        for dc in self.disk_controllers.iter().chain(alien_iter).cloned() {
            futs.push(async move { dc.run().await })
        }
        futs.fold(Ok(()), |s, n| async move { s.and(n) }).await?;
        let dur = Instant::now() - start;
        debug!("pearl backend init took {:?}", dur);
        Ok(())
    }

    async fn put(&self, op: Operation, key: BobKey, data: BobData) -> BackendResult<()> {
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

    async fn put_alien(&self, op: Operation, key: BobKey, data: BobData) -> BackendResult<()> {
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

    async fn get_alien(&self, op: Operation, key: BobKey) -> BackendResult<BobData> {
        debug!("Get[alien][{}] from pearl backend", key);
        if self.alien_disk_controller.can_process_operation(&op) {
            self.alien_disk_controller.get_alien(op, key).await
        } else {
            Err(Error::dc_is_not_available())
        }
    }

    async fn exist(&self, operation: Operation, keys: &[BobKey]) -> BackendResult<Vec<bool>> {
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

    async fn exist_alien(&self, operation: Operation, keys: &[BobKey]) -> BackendResult<Vec<bool>> {
        if self.alien_disk_controller.can_process_operation(&operation) {
            self.alien_disk_controller.exist(operation, &keys).await
        } else {
            Err(Error::dc_is_not_available())
        }
    }

    async fn shutdown(&self) {
        use futures::stream::FuturesUnordered;
        use std::iter::once;
        info!("begin shutdown");
        let futures = self
            .disk_controllers
            .iter()
            .chain(once(&self.alien_disk_controller))
            .map(|dc| async move { dc.shutdown().await })
            .collect::<FuturesUnordered<_>>();
        futures.collect::<()>().await;
        info!("shutting down done");
    }

    fn disk_controllers(&self) -> Option<(&[Arc<DiskController>], Arc<DiskController>)> {
        Some((&self.disk_controllers, self.alien_disk_controller.clone()))
    }

    async fn close_unneeded_active_blobs(&self, soft: usize, hard: usize) {
        for dc in self.disk_controllers.iter() {
            dc.close_unneeded_active_blobs(soft, hard).await;
        }
    }

    async fn delete(&self, op: Operation, key: BobKey) -> Result<u64, Error> {
        debug!("DELETE[{}] from pearl backend. operation: {:?}", key, op);
        let dc_option = self
            .disk_controllers
            .iter()
            .find(|dc| dc.can_process_operation(&op));

        if let Some(disk_controller) = dc_option {
            disk_controller.delete(op, key).await
        } else {
            Err(Error::dc_is_not_available())
        }
    }

    async fn delete_alien(&self, op: Operation, key: BobKey) -> Result<u64, Error> {
        debug!("DELETE[alien][{}] from pearl backend", key);
        if self.alien_disk_controller.can_process_operation(&op) {
            self.alien_disk_controller.delete_alien(op, key).await
        } else {
            Err(Error::dc_is_not_available())
        }
    }
}
