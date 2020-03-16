mod quorum;
mod simple;

pub(crate) use super::prelude::*;

mod prelude {
    pub(crate) use super::*;
}

use quorum::Quorum;
use simple::Quorum as SimpleQuorum;

pub(crate) trait Cluster {
    fn put_clustered_async(&self, key: BobKey, data: BobData) -> BackendPut;
    fn get_clustered_async(&self, key: BobKey) -> BackendGet;
    fn exist_clustered_async(&self, keys: &[BobKey]) -> BackendExist;
}

pub(crate) fn get_cluster(
    mapper: Arc<Virtual>,
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
