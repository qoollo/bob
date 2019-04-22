use crate::core::bob_client::BobClient;
use crate::core::configs::node::NodeConfig;
use crate::core::data::{
    print_vec, BobData, BobError, BobGetResult, BobKey, ClusterResult, Node, NodeDisk, VDisk, VDiskId, VDiskMapper
};
use crate::core::link_manager::{LinkManager, NodeLink};

use std::sync::Arc;
use tokio::prelude::*;

use futures::future::*;
use futures::stream::*;

#[derive(Clone)]
pub struct Cluster {
    pub vdisks: Vec<VDisk>,
}

#[derive(Clone)]
pub struct Sprinkler {
    cluster: Cluster,
    quorum: u8,
    link_manager: Arc<LinkManager>,
    mapper: VDiskMapper
}

pub struct SprinklerGetResult {
    pub data: Vec<u8>,
}
#[derive(Debug)]
pub struct SprinklerGetError {}

#[derive(Debug)]
pub struct SprinklerError {
    total_ops: u16,
    ok_ops: u16,
    quorum: u8,
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
    total_ops: u16,
    ok_ops: u16,
    quorum: u8,
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

impl Sprinkler {
    pub fn new(disks: &[VDisk], config: &NodeConfig) -> Sprinkler {
        let ex_cluster = Cluster {
            vdisks: disks.to_vec(),
        };
        let nodes: Vec<_> = ex_cluster
            .vdisks
            .iter()
            .flat_map(|vdisk| vdisk.replicas.iter().map(|nd| nd.node.clone()))
            .collect();

        let vdisk_count = ex_cluster.vdisks.iter().count() as u32;
        Sprinkler {
            quorum: config.quorum.unwrap(),
            cluster: ex_cluster,
            link_manager: Arc::new(LinkManager::new(
                nodes,
                config.check_interval(),
                config.timeout(),
            )),
            mapper: VDiskMapper::new(vdisk_count)
        }
    }

    pub fn get_periodic_tasks(
        &self,
        ex: tokio::runtime::TaskExecutor,
    ) -> Box<impl Future<Item = (), Error = ()>> {
        self.link_manager.get_checker_future(ex)
    }

    pub fn put_clustered(
        &self,
        key: BobKey,
        data: BobData,
    ) -> impl Future<Item = SprinklerResult, Error = SprinklerError> + 'static + Send {
        let target_nodes = self.calc_target_nodes(key);

        debug!(
            "PUT[{}]: Nodes for fan out: {:?}",
            key,
            print_vec(&target_nodes)
        );

        let reqs = Self::call_nodes(&mut self.get_connections(&target_nodes), |conn| {
            Box::new(conn.put(key, &data))
        });

        let l_quorum = self.quorum;
        Box::new(
            futures_unordered(reqs)
                .then(move |r| {
                    trace!("PUT[{}] Response from cluster {:?}", key, r);
                    ok::<_, ()>(r) // wrap all result kind to process it later
                })
                .fold(vec![], |mut acc, r| {
                    ok::<_, ()>({
                        acc.push(r);
                        acc
                    })
                })
                .then(move |acc| {
                    let res = acc.unwrap();
                    debug!("PUT[{}] cluster ans: {:?}", key, res);
                    let total_ops = res.iter().count();
                    let ok_count = res.iter().filter(|&r| r.is_ok()).count();
                    debug!(
                        "PUT[{}] total reqs: {} succ reqs: {} quorum: {}",
                        key, total_ops, ok_count, l_quorum
                    );
                    // TODO: send actuall list of vdisk it has been written on
                    if ok_count >= l_quorum as usize {
                        ok(SprinklerResult {
                            total_ops: total_ops as u16,
                            ok_ops: ok_count as u16,
                            quorum: l_quorum,
                        })
                    } else {
                        err(SprinklerError {
                            total_ops: total_ops as u16,
                            ok_ops: ok_count as u16,
                            quorum: l_quorum,
                        })
                    }
                }),
        )
    }

    pub fn get_clustered(
        &self,
        key: BobKey,
    ) -> impl Future<Item = ClusterResult<BobGetResult>, Error = BobError> + 'static + Send {
        let target_nodes = self.calc_target_nodes(key);

        debug!(
            "GET[{}]: Nodes for fan out: {:?}",
            key,
            print_vec(&target_nodes)
        );
        let reqs = Self::call_nodes(&mut self.get_connections(&target_nodes), |conn| {
            Box::new(conn.get(key))
        });

        Box::new(
            select_ok(reqs) // any result will enought
                .map(|(r, _)| r)
                .map_err(|_r| BobError::NotFound),
        )
    }

    fn calc_target_nodes(&self, key: BobKey) -> Vec<Node> {
        let vdisk_id = self.mapper.get_id(key);
        let target_vdisk = self
            .cluster
            .vdisks
            .iter()
            .find(|disk| disk.id == vdisk_id).unwrap();

        let mut target_nodes: Vec<_> = target_vdisk.replicas.iter().map(|nd| nd.node.clone())
            .collect();
        target_nodes.dedup();
        target_nodes
    }

    fn get_connections(&self, nodes: &[Node]) -> Vec<NodeLink> {
        nodes
            .iter()
            .map(|n| self.link_manager.clone().get_link(n))
            .collect()
    }

    fn call_nodes<F, T: 'static + Send>(
        links: &mut [NodeLink],
        mut f: F,
    ) -> Vec<Box<Future<Item = ClusterResult<T>, Error = ClusterResult<BobError>> + 'static + Send>>
    where
        F: FnMut(
            &mut BobClient,
        ) -> (Box<
            Future<Item = ClusterResult<T>, Error = ClusterResult<BobError>> + 'static + Send,
        >),
    {
        let t: Vec<_> = links
            .iter_mut()
            .map(move |nl| {
                let node = nl.node.clone();
                match &mut nl.conn {
                    Some(conn) => f(conn),
                    None => Box::new(err(ClusterResult {
                        result: BobError::Other(format!("No active connection {:?}", node)),
                        node,
                    })),
                }
            })
            .collect();
        t
    }
}
