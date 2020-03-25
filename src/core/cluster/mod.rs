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
    match config.cluster_policy() {
        "simple" => Arc::new(SimpleQuorum::new(mapper, config)),
        "quorum" => Arc::new(Quorum::new(backend, mapper, config.quorum.expect("quorum"))),
        p => panic!("unknown cluster policy: {}", p),
    }
}
