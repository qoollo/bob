mod quorum;
mod simple;

pub(crate) use super::prelude::*;

mod prelude {
    pub(crate) use super::*;
    pub(crate) use futures::stream::FuturesUnordered;
}

use quorum::QuorumCluster;
use simple::SimpleQuorumCluster;

trait Cluster {
    fn put_clustered_async(&self, key: BobKey, data: BobData) -> Put;
    fn get_clustered_async(&self, key: BobKey) -> Get;
}

fn get_cluster(
    _link: Arc<LinkManager>,
    mapper: Arc<VDiskMapper>,
    config: &NodeConfig,
    backend: Arc<Backend>,
) -> Arc<dyn Cluster + Send + Sync> {
    if config.cluster_policy() == "simple" {
        return Arc::new(SimpleQuorumCluster::new(mapper, config));
    }
    if config.cluster_policy() == "quorum" {
        return Arc::new(QuorumCluster::new(mapper, config, backend));
    }
    panic!("unknown cluster policy: {}", config.cluster_policy())
}
