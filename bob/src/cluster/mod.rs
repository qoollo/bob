mod operations;
mod quorum;
mod simple;

#[cfg(test)]
mod tests;

use crate::prelude::*;
use quorum::Quorum;
use simple::Quorum as SimpleQuorum;

#[async_trait]
pub(crate) trait Cluster {
    async fn put(&self, key: BobKey, data: &BobData) -> Result<(), Error>;
    async fn get(&self, key: BobKey) -> Result<BobData, Error>;
    async fn exist(&self, keys: &[BobKey]) -> Result<Vec<bool>, Error>;
    async fn delete(&self, key: BobKey, timestamp: u64) -> Result<(), Error>;
}

pub(crate) fn get_cluster(
    mapper: Arc<Virtual>,
    config: &NodeConfig,
    backend: Arc<Backend>,
) -> Arc<dyn Cluster + Send + Sync> {
    match config.cluster_policy() {
        "simple" => Arc::new(SimpleQuorum::new(mapper, config.quorum())),
        "quorum" => Arc::new(Quorum::new(backend, mapper, config.quorum())),
        p => panic!("unknown cluster policy: {}", p),
    }
}
