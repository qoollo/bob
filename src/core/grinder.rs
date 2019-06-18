use crate::core::{
    backend::backend::{Backend, BackendError, BackendGetResult, BackendResult},
    configs::node::NodeConfig,
    data::{BobData, BobError, BobGetResult, BobKey, BobOptions, ClusterResult, VDiskMapper},
    sprinkler::{Sprinkler, SprinklerError, SprinklerResult},
};

#[derive(Debug)]
pub enum ServeTypeOk<CT, BT> {
    Cluster(CT),
    Local(BT),
}

impl<CT, BT> ServeTypeOk<CT, BT> {
    pub fn is_cluster(&self) -> bool {
        match *self {
            ServeTypeOk::Cluster(_) => true,
            ServeTypeOk::Local(_) => false,
        }
    }
    pub fn is_local(&self) -> bool {
        !self.is_cluster()
    }
}

#[derive(Debug)]
pub enum ServeTypeError<CT, BT> {
    Cluster(CT),
    Local(BT),
}

impl<CT, BT> ServeTypeError<CT, BT> {
    pub fn is_cluster(&self) -> bool {
        match *self {
            ServeTypeError::Cluster(_) => true,
            ServeTypeError::Local(_) => false,
        }
    }
    pub fn is_local(&self) -> bool {
        !self.is_cluster()
    }
}

pub struct Grinder {
    pub backend: Backend,
    pub sprinkler: Sprinkler,
    mapper: VDiskMapper,
}

impl Grinder {
    pub fn new(mapper: VDiskMapper, config: &NodeConfig) -> Grinder {
        let backend = Backend::new(&mapper, config);
        Grinder {
            backend,
            sprinkler: Sprinkler::new(&mapper, config),
            mapper,
        }
    }
    pub async fn put(
        &self,
        key: BobKey,
        data: BobData,
        opts: BobOptions,
    ) -> Result<
        ServeTypeOk<SprinklerResult, BackendResult>,
        ServeTypeError<SprinklerError, BackendError>,
    > {
        if opts.contains(BobOptions::FORCE_NODE) {
            let op = self.mapper.get_operation(key);
            debug!(
                "PUT[{}] flag FORCE_NODE is on - will handle it by local node. Put params: {}",
                key, op
            );
            self.backend
                .put(&op, key, data)
                .0
                .await
                .map(|r| ServeTypeOk::Local(r))
                .map_err(|err| ServeTypeError::Local(err))
        } else {
            debug!("PUT[{}] will route to cluster", key);
            self.sprinkler
                .put_clustered(key, data)
                .await
                .map(|r| ServeTypeOk::Cluster(r))
                .map_err(|err| ServeTypeError::Cluster(err))
        }
    }

    pub async fn get(
        &self,
        key: BobKey,
        opts: BobOptions,
    ) -> Result<
        ServeTypeOk<ClusterResult<BobGetResult>, BackendGetResult>,
        ServeTypeError<BobError, BackendError>,
    > {
        if opts.contains(BobOptions::FORCE_NODE) {
            let op = self.mapper.get_operation(key);
            debug!(
                "GET[{}] flag FORCE_NODE is on - will handle it by local node. Get params: {}",
                key, op
            );
            self.backend
                .get(&op, key)
                .0
                .await
                .map(|r| ServeTypeOk::Local(r))
                .map_err(|err| ServeTypeError::Local(err))
        } else {
            debug!("GET[{}] will route to cluster", key);
            self.sprinkler
                .get_clustered(key)
                .await
                .map(|r| ServeTypeOk::Cluster(r))
                .map_err(|err| ServeTypeError::Cluster(err))
        }
    }

    pub async fn get_periodic_tasks(&self, ex: tokio::runtime::TaskExecutor) -> Result<(), ()> {
        self.sprinkler.get_periodic_tasks(ex).await
    }
}
