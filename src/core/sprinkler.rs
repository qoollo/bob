use crate::core::cluster::{get_cluster, Cluster};
use crate::core::configs::node::NodeConfig;
use crate::core::data::{BobData, BobError, BobGetResult, BobKey, ClusterResult, VDiskMapper};
use crate::core::link_manager::LinkManager;

use std::sync::Arc;
use tokio::prelude::*;

pub struct SprinklerGetResult {
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub struct SprinklerGetError {}

#[derive(Debug)]
pub struct SprinklerError {
    pub total_ops: u16,
    pub ok_ops: u16,
    pub quorum: u8,
}

impl std::fmt::Display for SprinklerError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "ok:{} total:{} q:{}",
            self.ok_ops, self.total_ops, self.quorum
        )
    }
}

#[derive(Debug)]
pub struct SprinklerResult {
    pub total_ops: u16,
    pub ok_ops: u16,
    pub quorum: u8,
}

impl std::fmt::Display for SprinklerResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "ok:{} total:{} q:{}",
            self.ok_ops, self.total_ops, self.quorum
        )
    }
}

pub struct Put(pub Box<dyn Future<Item = SprinklerResult, Error = SprinklerError> + Send>);
pub struct Get(pub Box<dyn Future<Item = ClusterResult<BobGetResult>, Error = BobError> + Send>);

#[derive(Clone)]
pub struct Sprinkler {
    link_manager: Arc<LinkManager>,
    mapper: VDiskMapper,
    cluster: Arc<dyn Cluster + Send + Sync>,
}

impl Sprinkler {
    pub fn new(mapper: &VDiskMapper, config: &NodeConfig) -> Sprinkler {
        let link = Arc::new(LinkManager::new(
            mapper.nodes(),
            config.check_interval(),
            config.timeout(),
        ));
        Sprinkler {
            link_manager: link.clone(),
            mapper: mapper.clone(),
            cluster: get_cluster(link, mapper, config),
        }
    }

    pub fn get_periodic_tasks(
        &self,
        ex: tokio::runtime::TaskExecutor,
    ) -> Box<impl Future<Item = (), Error = ()>> {
        self.link_manager.get_checker_future(ex)
    }

    pub fn put_clustered(&self, key: BobKey, data: BobData) -> Put {
        self.cluster.put_clustered(key, data)
    }

    pub fn get_clustered(&self, key: BobKey) -> Get {
        self.cluster.get_clustered(key)
    }
}
