use super::prelude::*;

pub struct Grinder {
    pub(crate) backend: Arc<Backend>,

    link_manager: Arc<LinkManager>,
    cluster: Arc<dyn Cluster + Send + Sync>,
}

impl Grinder {
    pub fn new(mapper: VDiskMapper, config: &NodeConfig) -> Grinder {
        let link = Arc::new(LinkManager::new(mapper.nodes(), config.check_interval()));
        let m_link = Arc::new(mapper);
        let backend = Arc::new(Backend::new(m_link.clone(), config));

        Grinder {
            backend: backend.clone(),
            link_manager: link,
            cluster: get_cluster(m_link, config, backend),
        }
    }

    pub(crate) async fn run_backend(&self) -> Result<(), BackendError> {
        self.backend.run_backend().await
    }

    pub(crate) async fn put(
        &self,
        key: BobKey,
        data: BobData,
        opts: BobOptions,
    ) -> Result<BackendPutResult, BackendError> {
        if opts.flags().contains(BobFlags::FORCE_NODE) {
            debug!(
                "PUT[{}] FORCE_NODE=true - will handle it by local node. Put params: {:?}",
                key, opts
            );
            CLIENT_PUT_COUNTER.count(1);
            let time = CLIENT_PUT_TIMER.start();

            let result = self.backend.put(key, data, opts).await.map_err(|err| {
                GRINDER_PUT_ERROR_COUNT_COUNTER.count(1);
                err
            });

            CLIENT_PUT_TIMER.stop(time);
            result
        } else {
            debug!("PUT[{}] will route to cluster", key);
            GRINDER_PUT_COUNTER.count(1);
            let time = GRINDER_PUT_TIMER.start();

            let result = self
                .cluster
                .put_clustered_async(key, data)
                .0
                .await
                .map_err(|err| {
                    GRINDER_PUT_ERROR_COUNT_COUNTER.count(1);
                    err
                });

            GRINDER_PUT_TIMER.stop(time);
            result
        }
    }

    pub(crate) async fn get(
        &self,
        key: BobKey,
        opts: &BobOptions,
    ) -> Result<BackendGetResult, BackendError> {
        if opts.flags().contains(BobFlags::FORCE_NODE) {
            CLIENT_GET_COUNTER.count(1);
            let time = CLIENT_GET_TIMER.start();

            debug!(
                "GET[{}] flag FORCE_NODE is on - will handle it by local node. Get params: {:?}",
                key, opts
            );
            let result = self.backend.get(key, opts).await.map_err(|err| {
                CLIENT_GET_ERROR_COUNT_COUNTER.count(1);
                err
            });

            CLIENT_GET_TIMER.stop(time);
            result
        } else {
            GRINDER_GET_COUNTER.count(1);
            let time = GRINDER_GET_TIMER.start();

            debug!("GET[{}] will route to cluster", key);
            let result = self
                .cluster
                .get_clustered_async(key)
                .0
                .await
                .map_err(|err| {
                    GRINDER_GET_ERROR_COUNT_COUNTER.count(1);
                    err
                });

            GRINDER_GET_TIMER.stop(time);
            result
        }
    }

    pub(crate) async fn exist(
        &self,
        keys: &[BobKey],
        opts: &BobOptions,
    ) -> Result<BackendExistResult, BackendError> {
        if opts.flags().contains(BobFlags::FORCE_NODE) {
            self.backend.exist(keys, opts).await
        } else {
            self.cluster.exist_clustered_async(keys).0.await
        }
    }

    #[inline]
    pub(crate) async fn get_periodic_tasks(&self, client_factory: Factory) -> Result<(), ()> {
        self.link_manager.get_checker_future(client_factory).await
    }
}
