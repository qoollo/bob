mod quorum;
mod simple;

use crate::core::{
    backend::core::{Backend, Get, Put},
    configs::node::NodeConfig,
    data::{BobData, BobKey},
    link_manager::LinkManager,
    mapper::VDiskMapper,
};
use std::sync::Arc;

use crate::core::cluster::{quorum::*, simple::*};

pub trait Cluster {
    fn put_clustered_async(&self, key: BobKey, data: BobData) -> Put;
    fn get_clustered_async(&self, key: BobKey) -> Get;
}

pub fn get_cluster(
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
