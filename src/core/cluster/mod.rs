mod quorum;
mod simple;

pub(crate) use super::prelude::*;

mod prelude {
    pub(crate) use super::*;
}

use quorum::Quorum;
use simple::Quorum as SimpleQuorum;

#[async_trait]
pub(crate) trait Cluster {
    async fn put(&self, key: BobKey, data: BobData) -> PutResult;
    async fn get(&self, key: BobKey) -> GetResult;
    async fn exist(&self, keys: &[BobKey]) -> ExistResult;
}

pub(crate) fn get_cluster(
    mapper: Arc<Virtual>,
    config: &NodeConfig,
    backend: Arc<Backend>,
) -> Arc<dyn Cluster + Send + Sync> {
    match config.cluster_policy() {
        "simple" => Arc::new(SimpleQuorum::new(mapper, config.quorum.expect("quorum"))),
        "quorum" => Arc::new(Quorum::new(backend, mapper, config.quorum.expect("quorum"))),
        p => panic!("unknown cluster policy: {}", p),
    }
}
