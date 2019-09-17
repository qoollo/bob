use crate::core::{
    backend,
    backend::core::{Backend, BackendGetResult, BackendPutResult},
    bob_client::BobClientFactory,
    cluster::{get_cluster, Cluster},
    configs::node::NodeConfig,
    data::{BobData, BobKey, BobOptions},
    link_manager::LinkManager,
    mapper::VDiskMapper,
    metrics::*,
};

use futures03::task::Spawn;
use std::sync::Arc;

#[derive(Debug)]
pub enum Error {
    NotFound,
    Other,
}

#[derive(Debug)]
pub enum BobError {
    Cluster(backend::Error),
    Local(backend::Error),
}

impl BobError {
    pub fn error(&self) -> Error {
        match self {
            BobError::Cluster(err) => self.match_error(err, false),
            BobError::Local(err) => self.match_error(err, true),
        }
    }

    fn match_error(&self, err: &backend::Error, _is_local: bool) -> Error {
        match err {
            backend::Error::KeyNotFound => Error::NotFound,
            _ => Error::Other,
        }
    }
    fn is_cluster(&self) -> bool {
        match *self {
            BobError::Cluster(_) => true,
            BobError::Local(_) => false,
        }
    }
    fn is_local(&self) -> bool {
        !self.is_cluster()
    }
}

impl std::fmt::Display for BobError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let dest = if self.is_local() { "local" } else { "cluster" };
        write!(f, "dest: {}, error: {}", dest, self)
    }
}

pub struct Grinder {
    pub backend: Arc<Backend>,

    link_manager: Arc<LinkManager>,
    cluster: Arc<dyn Cluster + Send + Sync>,
}

impl Grinder {
    pub fn new<TSpawner: Spawn + Clone + Send + 'static + Unpin + Sync>(
        mapper: VDiskMapper,
        config: &NodeConfig,
        spawner: TSpawner,
    ) -> Grinder {
        let link = Arc::new(LinkManager::new(mapper.nodes(), config.check_interval()));
        let m_link = Arc::new(mapper);
        let backend = Arc::new(Backend::new(m_link.clone(), config, spawner));

        Grinder {
            backend: backend.clone(),
            link_manager: link.clone(),
            cluster: get_cluster(link, m_link, config, backend),
        }
    }
    pub async fn run_backend(&self) -> Result<(), String> {
        self.backend.run_backend().await
    }
    pub async fn put(
        &self,
        key: BobKey,
        data: BobData,
        opts: BobOptions,
    ) -> Result<BackendPutResult, BobError> {
        if opts.contains(BobOptions::FORCE_NODE) {
            debug!(
                "PUT[{}] flag FORCE_NODE is on - will handle it by local node. Put params: {:?}",
                key, opts
            );
            CLIENT_PUT_COUNTER.count(1);
            let time = CLIENT_PUT_TIMER.start();

            let result = self.backend.put(key, data, opts).0.await.map_err(|err| {
                GRINDER_PUT_ERROR_COUNT_COUNTER.count(1);
                BobError::Local(err)
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
                    BobError::Cluster(err)
                });

            GRINDER_PUT_TIMER.stop(time);
            result
        }
    }

    pub async fn get(&self, key: BobKey, opts: BobOptions) -> Result<BackendGetResult, BobError> {
        if opts.contains(BobOptions::FORCE_NODE) {
            CLIENT_GET_COUNTER.count(1);
            let time = CLIENT_GET_TIMER.start();

            debug!(
                "GET[{}] flag FORCE_NODE is on - will handle it by local node. Get params: {:?}",
                key, opts
            );
            let result = self.backend.get(key, opts).0.await.map_err(|err| {
                CLIENT_GET_ERROR_COUNT_COUNTER.count(1);
                BobError::Local(err)
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
                    BobError::Cluster(err)
                });

            GRINDER_GET_TIMER.stop(time);
            result
        }
    }

    pub async fn get_periodic_tasks<S>(
        &self,
        client_factory: BobClientFactory,
        spawner: S,
    ) -> Result<(), ()>
    where
        S: Spawn + Clone + Send + 'static + Unpin + Sync,
    {
        self.link_manager
            .get_checker_future(client_factory, spawner)
            .await
    }
}
