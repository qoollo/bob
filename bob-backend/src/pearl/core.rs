use futures::{future::ready, Future};
use std::iter::once;

use crate::{
    pearl::{hooks::BloomFilterMemoryLimitHooks, utils::Utils},
    prelude::*,
};

use super::{
    data::Key, disk_controller::logger::DisksEventsLogger, disk_controller::DiskController,
    hooks::SimpleHolder, settings::Settings, Holder,
};
use crate::core::{BackendStorage, MetricsProducer, Operation};

pub type BackendResult<T> = std::result::Result<T, Error>;
pub type PearlStorage = Storage<Key>;

#[derive(Clone, Debug)]
pub struct Pearl {
    disk_controllers: Arc<[Arc<DiskController>]>,
    alien_disk_controller: Arc<DiskController>,
    bloom_filter_memory_limit: Option<usize>,
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
        let iodriver = get_io_driver(&config.pearl());
        let data = settings
            .clone()
            .read_group_from_disk(config, run_sem.clone(), logger.clone(), iodriver.clone())
            .await;
        let disk_controllers: Arc<[_]> = Arc::from(data.as_slice());
        trace!("count vdisk groups: {}", disk_controllers.len());

        let alien_disk_controller = settings
            .clone()
            .read_alien_directory(config, run_sem, logger, iodriver)
            .await;

        let pearl = Self {
            disk_controllers,
            alien_disk_controller,
            bloom_filter_memory_limit: config.bloom_filter_memory_limit(),
        };
        Ok(pearl)
    }

    async fn for_each_holder<F, Fut>(&self, f: F)
    where
        F: Fn(&Holder) -> Fut + Clone,
        Fut: Future<Output = ()>,
    {
        self.all_disk_controllers()
            .map(|dc| dc.for_each_holder(f.clone()))
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<()>>()
            .await;
    }

    async fn collect_simple_holders(&self) -> Vec<SimpleHolder> {
        let res = RwLock::new(vec![]);
        self.for_each_holder(|h| {
            let h = SimpleHolder::from(h);
            async {
                if h.is_ready().await {
                    res.write().await.push(h);
                }
            }
        })
        .await;
        res.into_inner()
    }

    fn all_disk_controllers(&self) -> impl Iterator<Item = &Arc<DiskController>> {
        self.disk_controllers
            .iter()
            .chain(once(&self.alien_disk_controller))
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
            .all_disk_controllers()
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

    async fn disk_used_by_disk(&self) -> HashMap<DiskPath, u64> {
        let futs: FuturesUnordered<_> = self
            .all_disk_controllers()
            .cloned()
            .map(|dc| async move { (dc.disk().clone(), dc.disk_used().await) })
            .collect();
        futs.fold(HashMap::new(), |mut m, (n, u)| {
            m.entry(n).and_modify(|s| *s = *s + u).or_insert(u);
            ready(m)
        })
        .await
    }

    async fn corrupted_blobs_count(&self) -> usize {
        let futs: FuturesUnordered<_> = self
            .disk_controllers
            .iter()
            .cloned()
            .map(|dc| async move { dc.corrupted_blobs_count().await })
            .collect();
        let cnt = futs.fold(0, |cnt, dc_cnt| ready(cnt + dc_cnt)).await;
        cnt
    }
}

#[async_trait]
impl BackendStorage for Pearl {
    async fn run_backend(&self) -> AnyResult<()> {
        // vec![vec![]; n] macro requires Clone trait, and future does not implement it
        let start = Instant::now();
        let futs = FuturesUnordered::new();
        let postprocessor = BloomFilterMemoryLimitHooks::new(self.bloom_filter_memory_limit);
        for dc in self.all_disk_controllers() {
            let pp = postprocessor.clone();
            futs.push(async move { dc.run(pp).await });
        }
        futs.fold(Ok(()), |s, n| async move { s.and(n) }).await?;
        let dur = Instant::now() - start;
        debug!("pearl backend init took {:?}", dur);
        Ok(())
    }

    async fn put(&self, op: Operation, key: BobKey, data: &BobData) -> BackendResult<()> {
        debug!("PUT[{}] to pearl backend. operation: {:?}", key, op);
        let dc_option = self
            .disk_controllers
            .iter()
            .find(|dc| dc.can_process_operation(&op));
        if let Some(disk_controller) = dc_option {
            disk_controller
                .put(op, key, data)
                .await
        } else {
            debug!(
                "PUT[{}] Cannot find disk_controller, operation: {:?}",
                key, op
            );
            Err(Error::dc_is_not_available())
        }
    }

    async fn put_alien(&self, op: Operation, key: BobKey, data: &BobData) -> BackendResult<()> {
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
            self.alien_disk_controller
                .exist_alien(operation, keys)
                .await
        } else {
            Err(Error::dc_is_not_available())
        }
    }

    async fn delete(&self, op: Operation, key: BobKey, meta: &BobMeta) -> Result<u64, Error> {
        debug!("DELETE[{}] from pearl backend. operation: {:?}", key, op);
        let dc_option = self
            .disk_controllers
            .iter()
            .find(|dc| dc.can_process_operation(&op));

        if let Some(disk_controller) = dc_option {
            disk_controller.delete(op, key, meta).await
        } else {
            Err(Error::dc_is_not_available())
        }
    }

    async fn delete_alien(&self, op: Operation, key: BobKey, meta: &BobMeta, force_delete: bool) -> Result<u64, Error> {
        debug!("DELETE[alien][{}] from pearl backend", key);
        if self.alien_disk_controller.can_process_operation(&op) {
            self.alien_disk_controller.delete_alien(op, key, meta, force_delete).await
        } else {
            Err(Error::dc_is_not_available())
        }
    }

    async fn shutdown(&self) {
        use futures::stream::FuturesUnordered;
        info!("begin shutdown");
        let futures = self
            .all_disk_controllers()
            .map(|dc| async move { dc.shutdown().await })
            .collect::<FuturesUnordered<_>>();
        futures.collect::<()>().await;
        info!("shutting down done");
    }

    fn disk_controllers(&self) -> Option<(&[Arc<DiskController>], Arc<DiskController>)> {
        Some((&self.disk_controllers, self.alien_disk_controller.clone()))
    }

    async fn close_unneeded_active_blobs(&self, soft: usize, hard: usize) {
        for dc in self.all_disk_controllers() {
            dc.close_unneeded_active_blobs(soft, hard).await;
        }
    }

    async fn close_oldest_active_blob(&self) -> Option<usize> {
        let mut oldest: Option<Holder> = None;
        for dc in self.all_disk_controllers() {
            if let Some(holder) = dc.find_oldest_inactive_holder().await {
                if holder.end_timestamp()
                    < oldest
                        .as_ref()
                        .map(|h| h.end_timestamp())
                        .unwrap_or(u64::MAX)
                {
                    oldest = Some(holder);
                }
            }
        }

        if let Some(h) = oldest {
            let memory = h.active_index_memory().await;
            h.close_active_blob().await;
            Some(memory)
        } else {
            None
        }
    }

    async fn free_least_used_resources(&self) -> Option<usize> {
        let mut least_modified: Option<Holder> = None;
        let mut min_modification = u64::MAX;
        for dc in self.all_disk_controllers() {
            if let Some(holder) = dc.find_least_modified_freeable_holder().await {
                let modification = holder.last_modification();
                if modification < min_modification {
                    least_modified = Some(holder);
                    min_modification = modification;
                }
            }
        }

        if let Some(h) = least_modified {
            let freed = h.free_excess_resources().await;
            Some(freed)
        } else {
            None
        }
    }

    async fn offload_old_filters(&self, limit: usize) {
        Utils::offload_old_filters(self.collect_simple_holders().await, limit).await;
    }

    async fn filter_memory_allocated(&self) -> usize {
        let mut memory = 0;
        for dc in self.all_disk_controllers() {
            memory += dc.filter_memory_allocated().await;
        }
        memory
    }

    async fn remount_vdisk(&self, vdisk_id: u32) -> AnyResult<()> {
        let (dcs, _) = self.disk_controllers().ok_or(Error::internal())?;
        let needed_dc = dcs
            .iter()
            .find(|dc| dc.vdisks().iter().any(|&vd| vd == vdisk_id))
            .ok_or(Error::vdisk_not_found(vdisk_id))?;
        let mut group = needed_dc.vdisk_group(vdisk_id).await?;
        let postprocessor = BloomFilterMemoryLimitHooks::new(self.bloom_filter_memory_limit);
        group.remount(postprocessor).await
    }
}

#[cfg(not(feature = "async-io"))]
fn get_io_driver(pearl_config: &PearlConfig) -> IoDriver {
    if pearl_config.is_aio_enabled() {
        warn!("async io feature is not enabled, ignoring aio flag from config");
    }
    IoDriver::new_sync()
}

#[cfg(feature = "async-io")]
fn get_io_driver(pearl_config: &PearlConfig) -> IoDriver {
    let iodriver = if pearl_config.is_aio_enabled() {
        info!("bob will start with AIO - async fs io api");
        IoDriver::new_async()
            .unwrap_or_else(|e| {
                warn!("bob will start with standard sync fs io api, can't start with AIO, cause: {:?}", e);
                IoDriver::new_sync()
            })
    } else {
        info!("bob will start with standard sync fs io api, cause: async io disabled in config");
        IoDriver::new_sync()
    };
    iodriver
}
