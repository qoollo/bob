use crate::core::data::{
    print_vec, BobData, BobError, BobKey, Node, VDiskMapper
};
use crate::core::configs::node::NodeConfig;
use crate::core::sprinkler::{SprinklerResult, SprinklerError, Put, Get};
use crate::core::link_manager::{LinkManager};
use futures::future::*;
use futures::stream::*;
use std::sync::Arc;

pub trait Cluster {
    fn put_clustered(&self, key: BobKey, data: BobData) -> Put;
    fn get_clustered(&self, key: BobKey) -> Get;
}

pub fn get_cluster(link: Arc<LinkManager>, mapper: &VDiskMapper, config: &NodeConfig) -> Arc<dyn Cluster + Send + Sync> {
    if config.cluster_policy() == "quorum" 
    {
        return Arc::new(QuorumCluster::new(link.clone(), mapper, config));
    }
    panic!("unknown cluster policy: {}", config.cluster_policy())
}

pub struct QuorumCluster {
    link_manager: Arc<LinkManager>,
    mapper: VDiskMapper,
    quorum: u8,
}

impl QuorumCluster {
    pub fn new(link_manager: Arc<LinkManager>, mapper: &VDiskMapper, config: &NodeConfig) ->Self{
        QuorumCluster{
            quorum: config.quorum.unwrap(),
            link_manager,
            mapper: mapper.clone(),
        }
    }

    fn calc_target_nodes(&self, key: BobKey) -> Vec<Node> {
        let target_vdisk = self.mapper.get_vdisk(key);

        let mut target_nodes: Vec<_> = target_vdisk
            .replicas
            .iter()
            .map(|nd| nd.node.clone())
            .collect();
        target_nodes.dedup();
        target_nodes
    }
}

impl Cluster for QuorumCluster {
    fn put_clustered(&self, key: BobKey, data: BobData) -> Put{
        Put({
            let target_nodes = self.calc_target_nodes(key);

            debug!(
                "PUT[{}]: Nodes for fan out: {:?}",
                key,
                print_vec(&target_nodes)
            );

            let reqs = self.link_manager.call_nodes(&target_nodes, |conn| {
                conn.put(key, &data).0
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
        })
    }

    fn get_clustered(&self, key: BobKey) -> Get{
        Get({
            let target_nodes = self.calc_target_nodes(key);

            debug!(
                "GET[{}]: Nodes for fan out: {:?}",
                key,
                print_vec(&target_nodes)
            );
            let reqs = self.link_manager.call_nodes(&target_nodes, |conn| {
                conn.get(key).0
            });

            Box::new(
                select_ok(reqs) // any result will enought
                    .map(|(r, _)| r)
                    .map_err(|_r| BobError::NotFound),
            )
        })
    }
}