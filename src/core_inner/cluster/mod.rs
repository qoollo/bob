mod quorum;
mod simple;

pub(crate) use super::prelude::*;

mod prelude {
    pub(crate) use super::*;

    pub(crate) use futures::stream::FuturesUnordered;
}

use quorum::Quorum;
use simple::Quorum as SimpleQuorum;

pub(crate) trait Cluster {
    fn put_clustered_async(&self, key: BobKey, data: BobData) -> BackendPut;
    fn get_clustered_async(&self, key: BobKey) -> BackendGet;
}

pub(crate) fn get_cluster(
    // _link: Arc<LinkManager>,
    mapper: Arc<VDiskMapper>,
    config: &NodeConfig,
    backend: Arc<Backend>,
) -> Arc<dyn Cluster + Send + Sync> {
    if config.cluster_policy() == "simple" {
        return Arc::new(SimpleQuorum::new(mapper, config));
    }
    if config.cluster_policy() == "quorum" {
        return Arc::new(Quorum::new(mapper, config, backend));
    }
    panic!("unknown cluster policy: {}", config.cluster_policy())
}
