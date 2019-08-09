mod simple;

use crate::core::{
    backend::core::{Get, Put},
    configs::node::NodeConfig,
    data::{BobData, BobKey, VDiskMapper},
    link_manager::LinkManager,
};
use std::sync::Arc;

use crate::core::cluster::simple::*;

pub trait Cluster {
    fn put_clustered(&self, key: BobKey, data: BobData) -> Put;
    fn get_clustered(&self, key: BobKey) -> Get;
}

pub fn get_cluster(
    link: Arc<LinkManager>,
    mapper: &VDiskMapper,
    config: &NodeConfig,
) -> Arc<dyn Cluster + Send + Sync> {
    if config.cluster_policy() == "simple" {
        return Arc::new(SimpleQuorumCluster::new(link.clone(), mapper, config));
    }
    panic!("unknown cluster policy: {}", config.cluster_policy())
}
