use crate::core::cluster::{get_cluster, Cluster};
use crate::core::configs::node::NodeConfig;
use crate::core::data::{BobData, BobError, BobGetResult, BobKey, ClusterResult, VDiskMapper};
use crate::core::link_manager::LinkManager;

use std::sync::Arc;

use futures03::Future as NewFuture;
use std::pin::Pin;

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

pub struct Put(
    pub Pin<Box<dyn NewFuture<Output = Result<SprinklerResult, SprinklerError>> + Send>>,
);
pub type PutResult = Result<SprinklerResult, SprinklerError>;
pub struct Get(
    pub Pin<Box<dyn NewFuture<Output = Result<ClusterResult<BobGetResult>, BobError>> + Send>>,
);
pub type GetResult = Result<ClusterResult<BobGetResult>, BobError>;

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

    pub async fn get_periodic_tasks(&self, ex: tokio::runtime::TaskExecutor) -> Result<(), ()> {
        self.link_manager.get_checker_future(ex).await
    }

    pub async fn put_clustered(&self, key: BobKey, data: BobData) -> PutResult {
        self.cluster.put_clustered(key, data).0.await
    }

    pub async fn get_clustered(&self, key: BobKey) -> GetResult {
        self.cluster.get_clustered(key).0.await
    }
}
