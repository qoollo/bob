use futures::future::ready;
use std::iter::once;

use crate::{pearl::Holder, prelude::*};

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
    filter_memory_limit: Option<usize>,
}

impl Pearl {
    pub async fn new(mapper: Arc<Virtual>, config: &NodeConfig) -> BackendResult<Self> {
        debug!("initializing pearl backend");
        let settings = Arc::new(Settings::new(config, mapper));
        let logfile = config.pearl().disks_events_logfile();
        let logger = DisksEventsLogger::new(logfile).await.map_err(|e| {
            Error::disk_events_logger("disk events logger initialization failed", e)
        })?;

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

        let pearl = Self {
            settings,
            disk_controllers,
            alien_disk_controller,
            node_name: config.name().to_string(),
            init_par_degree: config.init_par_degree(),
            filter_memory_limit: config.filter_memory_limit(),
        };
        Ok(pearl)
    }

    async fn offload_old_filters_in_holders(holders: &mut [Holder], limit: usize) {
        let mut holders = holders
            .iter_mut()
            .map(|h| async { (h.filter_memory_allocated().await, h) })
            .collect::<FuturesUnordered<_>>()
            .fold(vec![], |mut acc, x| async move {
                acc.push(x);
                acc
            })
            .await;
        let mut current_size = holders.iter().map(|x| x.0).sum::<usize>();
        if current_size < limit {
            return;
        }
        holders.sort_by_key(|h| h.1.end_timestamp());
        for (size, holder) in holders {
            if current_size < limit {
                break;
            }
            holder.offload_filter().await;
            let new_size = holder.filter_memory_allocated().await;
            current_size = current_size.saturating_sub(size.saturating_sub(new_size));
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
        let memory_limit = self.filter_memory_limit.clone();
        let futs = FuturesUnordered::new();
        let alien_iter = once(&self.alien_disk_controller);
        let holders = Arc::new(RwLock::new(vec![]));
        for dc in self.disk_controllers.iter().chain(alien_iter).cloned() {
            let holders = holders.clone();
            futs.push(async move {
                if let Some(limit) = memory_limit {
                    dc.run(|h| {
                        let holders = holders.clone();
                        let holder = h.clone();
                        async move {
                            let mut holders_lock = holders.write().await;
                            holders_lock.push(holder);
                            if holders_lock.len() % 100 == 0 {
                                Self::offload_old_filters_in_holders(&mut holders_lock, limit)
                                    .await;
                            }
                        }
                    })
                    .await
                } else {
                    dc.run(|_| async {}).await
                }
            })
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
            disk_controller.exist(operation, keys).await
        } else {
            Err(Error::dc_is_not_available())
        }
    }

    async fn exist_alien(&self, operation: Operation, keys: &[BobKey]) -> BackendResult<Vec<bool>> {
        if self.alien_disk_controller.can_process_operation(&operation) {
            self.alien_disk_controller.exist(operation, keys).await
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

    async fn offload_old_filters(&self, limit: usize) {
        let mut holders = vec![];
        for dc in self
            .disk_controllers
            .iter()
            .chain(Some(&self.alien_disk_controller))
        {
            for group in dc.groups().read().await.iter() {
                for holder in group.holders().read().await.iter() {
                    holders.push(holder.clone());
                }
            }
        }
        Self::offload_old_filters_in_holders(&mut holders, limit).await;
    }

    async fn filter_memory_allocated(&self) -> usize {
        let mut memory = 0;
        for dc in self
            .disk_controllers
            .iter()
            .chain(Some(&self.alien_disk_controller))
        {
            memory += dc.filter_memory_allocated().await;
        }
        memory
    }
}
