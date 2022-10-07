use crate::prelude::*;

use crate::{
    cleaner::Cleaner,
    cluster::{get_cluster, Cluster},
    counter::Counter as BlobsCounter,
    hw_metrics_collector::HWMetricsCollector,
    link_manager::LinkManager,
};

use bob_common::metrics::{
    CLIENT_DELETE_COUNTER, CLIENT_DELETE_ERROR_COUNT_COUNTER, CLIENT_DELETE_TIMER,
    GRINDER_DELETE_COUNTER, GRINDER_DELETE_ERROR_COUNT_COUNTER, GRINDER_DELETE_TIMER,
};
use metrics::histogram as timing;

/// Struct for cooperation backend, link manager and cluster
pub struct Grinder {
    backend: Arc<Backend>,
    link_manager: Arc<LinkManager>,
    cluster: Arc<dyn Cluster + Send + Sync>,
    cleaner: Arc<Cleaner>,
    counter: Arc<BlobsCounter>,
    node_config: NodeConfig,
    hw_counter: Arc<HWMetricsCollector>,
}

impl Grinder {
    /// Creates new instance of the Grinder
    pub async fn new(mapper: Virtual, config: &NodeConfig) -> Grinder {
        let nodes = mapper.nodes().values().cloned().collect::<Vec<_>>();
        let link_manager = Arc::new(LinkManager::new(nodes.as_slice(), config.check_interval()));
        let mapper = Arc::new(mapper);
        let backend = Arc::new(Backend::new(mapper.clone(), config).await);
        let cleaner = Cleaner::new(
            config.cleanup_interval(),
            config.open_blobs_soft(),
            config.hard_open_blobs(),
            config.bloom_filter_memory_limit(),
            config.index_memory_limit(),
            config.index_memory_limit_soft(),
        );
        let cleaner = Arc::new(cleaner);
        let hw_counter = Arc::new(HWMetricsCollector::new(
            mapper.clone(),
            Duration::from_secs(60),
        ));

        let counter = Arc::new(BlobsCounter::new(config.count_interval()));
        Grinder {
            backend: backend.clone(),
            link_manager,
            cluster: get_cluster(mapper, config, backend),
            cleaner,
            counter,
            node_config: config.clone(),
            hw_counter,
        }
    }

    pub(crate) fn backend(&self) -> &Arc<Backend> {
        &self.backend
    }

    pub(crate) async fn run_backend(&self) -> Result<()> {
        self.backend.run_backend().await
    }

    pub(crate) fn node_config(&self) -> &NodeConfig {
        &self.node_config
    }

    pub(crate) fn hw_counter(&self) -> &HWMetricsCollector {
        &self.hw_counter
    }

    pub(crate) async fn put(
        &self,
        key: BobKey,
        data: BobData,
        opts: BobOptions,
    ) -> Result<(), Error> {
        let sw = Stopwatch::start_new();
        if opts.flags().contains(BobFlags::FORCE_NODE) {
            trace!(">>>- - - - - GRINDER PUT START - - - - -");
            debug!(
                "PUT[{}] FORCE_NODE=true - will handle it by local node. Put params: {:?}",
                key, opts
            );
            counter!(CLIENT_PUT_COUNTER, 1);
            let time = Instant::now();

            let result = self.backend.put(key, data, opts).await;
            trace!(
                "backend processed put, /{:.3}ms/",
                sw.elapsed().as_secs_f64() * 1000.0
            );
            if result.is_err() {
                counter!(CLIENT_PUT_ERROR_COUNT_COUNTER, 1);
            }

            timing!(CLIENT_PUT_TIMER, time.elapsed().as_nanos() as f64);
            trace!("<<<- - - - - GRINDER PUT FINISH - - - - -");
            result
        } else {
            debug!("PUT[{}] will route to cluster", key);
            counter!(GRINDER_PUT_COUNTER, 1);
            let time = Instant::now();

            let result = self.cluster.put(key, data).await;
            if result.is_err() {
                counter!(GRINDER_PUT_ERROR_COUNT_COUNTER, 1);
            }

            timing!(GRINDER_PUT_TIMER, time.elapsed().as_nanos() as f64);
            trace!(">>>- - - - - GRINDER PUT FINISH - - - - -");
            result
        }
    }

    pub(crate) async fn get(&self, key: BobKey, opts: &BobOptions) -> Result<BobData, Error> {
        trace!(">>>- - - - - GRINDER GET START - - - - -");
        let sw = Stopwatch::start_new();
        if opts.flags().contains(BobFlags::FORCE_NODE) {
            trace!(
                "pass request to backend, /{:.3}ms/",
                sw.elapsed().as_secs_f64() * 1000.0
            );
            counter!(CLIENT_GET_COUNTER, 1);
            let time = Instant::now();

            debug!(
                "GET[{}] flag FORCE_NODE is on - will handle it by local node. Get params: {:?}",
                key, opts
            );
            let result = self.backend.get(key, opts).await;
            trace!(
                "backend processed get, /{:.3}ms/",
                sw.elapsed().as_secs_f64() * 1000.0
            );
            if result.is_err() {
                counter!(CLIENT_GET_ERROR_COUNT_COUNTER, 1);
            }

            timing!(CLIENT_GET_TIMER, time.elapsed().as_nanos() as f64);
            trace!(">>>- - - - - GRINDER GET FINISHED - - - - -");
            result
        } else {
            trace!(
                "pass request to cluster, /{:.3}ms/",
                sw.elapsed().as_secs_f64() * 1000.0
            );
            counter!(GRINDER_GET_COUNTER, 1);
            let time = Instant::now();
            debug!("GET[{}] will route to cluster", key);
            let result = self.cluster.get(key).await;
            trace!(
                "cluster processed get, /{:.3}ms/",
                sw.elapsed().as_secs_f64() * 1000.0
            );
            if result.is_err() {
                counter!(GRINDER_GET_ERROR_COUNT_COUNTER, 1);
            }
            timing!(GRINDER_GET_TIMER, time.elapsed().as_nanos() as f64);
            trace!(">>>- - - - - GRINDER GET FINISHED - - - - -");
            result
        }
    }

    pub(crate) async fn exist(
        &self,
        keys: &[BobKey],
        opts: &BobOptions,
    ) -> Result<Vec<bool>, Error> {
        let sw = Stopwatch::start_new();
        if opts.flags().contains(BobFlags::FORCE_NODE) {
            counter!(CLIENT_EXIST_COUNTER, 1);
            let time = Instant::now();
            let result = self.backend.exist(keys, opts).await;
            trace!(
                "backend processed exist, /{:.3}ms/",
                sw.elapsed().as_secs_f64() * 1000.0
            );
            if result.is_err() {
                counter!(CLIENT_EXIST_ERROR_COUNT_COUNTER, 1);
            }
            timing!(CLIENT_EXIST_TIMER, time.elapsed().as_nanos() as f64);
            result
        } else {
            counter!(GRINDER_EXIST_COUNTER, 1);
            let time = Instant::now();
            let result = self.cluster.exist(keys).await;
            trace!(
                "cluster processed exist, /{:.3}ms/",
                sw.elapsed().as_secs_f64() * 1000.0
            );
            if result.is_err() {
                counter!(GRINDER_EXIST_ERROR_COUNT_COUNTER, 1);
            }
            timing!(GRINDER_EXIST_TIMER, time.elapsed().as_nanos() as f64);
            result
        }
    }

    #[inline]
    pub(crate) fn run_periodic_tasks(&self, client_factory: Factory) {
        self.link_manager.spawn_checker(client_factory);
        self.cleaner
            .spawn_task(self.cleaner.clone(), self.backend.clone());
        self.counter.spawn_task(self.backend.clone());
        self.hw_counter.spawn_task();
    }

    pub(crate) async fn delete(&self, key: BobKey, options: BobOptions) -> Result<(), Error> {
        trace!(">>>- - - - - GRINDER DELETE START - - - - -");
        let result = if options.flags().contains(BobFlags::FORCE_NODE) {
            counter!(CLIENT_DELETE_COUNTER, 1);
            let sw = Stopwatch::start_new();
            trace!(
                "pass delete request to backend, /{:.3}ms/",
                sw.elapsed().as_secs_f64() * 1000.0
            );
            let result = self.backend.delete(key, options).await;
            trace!(
                "backend processed delete, /{:.3}ms/",
                sw.elapsed().as_secs_f64() * 1000.0
            );
            if result.is_err() {
                counter!(CLIENT_DELETE_ERROR_COUNT_COUNTER, 1);
            }
            timing!(CLIENT_DELETE_TIMER, sw.elapsed().as_nanos() as f64);
            result
        } else {
            counter!(GRINDER_DELETE_COUNTER, 1);
            let sw = Stopwatch::start_new();
            let result = self.cluster.delete(key).await;
            trace!(
                "cluster processed delete, /{:.3}ms/",
                sw.elapsed().as_secs_f64() * 1000.0
            );
            if result.is_err() {
                counter!(GRINDER_DELETE_ERROR_COUNT_COUNTER, 1);
            }
            timing!(GRINDER_DELETE_TIMER, sw.elapsed().as_nanos() as f64);
            result
        };
        trace!(">>>- - - - - GRINDER DELETE FINISHED - - - - -");
        self.cleaner.request_index_cleanup();
        result
    }
}

impl Debug for Grinder {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Grinder")
            .field("backend", &self.backend)
            .field("link_manager", &self.link_manager)
            .field("cluster", &"..")
            .finish()
    }
}
