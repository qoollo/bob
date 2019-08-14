mod simple;
mod quorum;

use crate::core::{
    backend::core::{Get, Put, Backend},
    configs::node::NodeConfig,
    data::{BobData, BobKey},
    mapper::VDiskMapper,
    link_manager::LinkManager,
};
use std::sync::Arc;

use crate::core::cluster::{
    simple::*,
    quorum::*,
};

pub trait Cluster {
    fn put_clustered(&self, key: BobKey, data: BobData) -> Put;
    fn get_clustered(&self, key: BobKey) -> Get;
}

pub fn get_cluster(
    link: Arc<LinkManager>,
    mapper: &VDiskMapper,
    config: &NodeConfig,
    backend: Arc<Backend>,
) -> Arc<dyn Cluster + Send + Sync> {
    if config.cluster_policy() == "simple" {
        return Arc::new(SimpleQuorumCluster::new(link.clone(), mapper, config));
    }
    if config.cluster_policy() == "quorum" {
        return Arc::new(QuorumCluster::new(link.clone(), mapper, config, backend));
    }
    panic!("unknown cluster policy: {}", config.cluster_policy())
}
