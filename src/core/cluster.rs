use crate::core::configs::node::NodeConfig;
use crate::core::data::{print_vec, BobData, BobError, BobKey, Node, VDiskMapper, BobPutResult, BobGetResult, ClusterResult};
use crate::core::link_manager::LinkManager;
use crate::core::sprinkler::{Get, Put, SprinklerError, SprinklerResult};
use futures::future::*;
use futures::stream::*;
use std::sync::Arc;

use futures03::future::err as err2;
use futures03::future::ok as ok2;
use futures03::future::ready as ready2;
use futures03::Future as NewFuture;
use futures03::future::{TryFutureExt, FutureExt};
use futures03::compat::Future01CompatExt;
use futures03::stream::FuturesUnordered as unordered;
use futures03::stream::StreamExt;
use std::pin::Pin;
use crate::core::bob_client::{BobClient};

pub trait Cluster {
    fn put_clustered(&self, key: BobKey, data: BobData) -> Put;
    fn get_clustered(&self, key: BobKey) -> Get;
}

pub fn get_cluster(
    link: Arc<LinkManager>,
    mapper: &VDiskMapper,
    config: &NodeConfig,
) -> Arc<dyn Cluster + Send + Sync> {
    if config.cluster_policy() == "quorum" {
        return Arc::new(QuorumCluster::new(link.clone(), mapper, config));
    }
    panic!("unknown cluster policy: {}", config.cluster_policy())
}

pub struct QuorumCluster {
    link_manager: Arc<LinkManager>,
    mapper: VDiskMapper,
    quorum: u8,
}
// a collection of type 
// `futures_util::stream::futures_unordered::FuturesUnordered<
//     std::pin::Pin<std::boxed::Box<(dyn bitflags::core::future::future::Future<
//         Output = std::result::Result<core::data::ClusterResult<core::data::BobPutResult>, 
//                                     core::data::ClusterResult<core::data::BobError>>> + std::marker::Send)>>>` 
// cannot be built from an iterator over elements of type `
//     &std::pin::Pin<std::boxed::Box<dyn bitflags::core::future::future::Future<
//         Output = std::result::Result<core::data::ClusterResult<core::data::BobPutResult>, 
//                                     core::data::ClusterResult<core::data::BobError>>> + std::marker::Send>>`

impl QuorumCluster {
    pub fn new(link_manager: Arc<LinkManager>, mapper: &VDiskMapper, config: &NodeConfig) -> Self {
        QuorumCluster {
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
    fn put_clustered(&self, key: BobKey, data: BobData) -> Put {
        Put({
            let target_nodes = self.calc_target_nodes(key);

            debug!(
                "PUT[{}]: Nodes for fan out: {:?}",
                key,
                print_vec(&target_nodes)
            );

            let reqs = self
                .link_manager
                .call_nodes2(&target_nodes, |conn| conn.put(key, &data).0.compat().boxed());

            let mut t = unordered::new();
            t = reqs.into_iter().collect();
            
            let l_quorum = self.quorum;
            t
                .then(move |r| {
                    trace!("PUT[{}] Response from cluster {:?}", key, r);
                    ok2::<_, ()>(r) // wrap all result kind to process it later
                })
                .fold(vec![], |mut acc, r| {
                    acc.push(r);
                    ready2(acc)
                })
                .then(move |acc| {
                    debug!("PUT[{}] cluster ans: {:?}", key, acc);
                    let total_ops = acc.iter().count();
                    let ok_count = acc.iter().filter(|&r| r.is_ok()).count();
                    debug!(
                        "PUT[{}] total reqs: {} succ reqs: {} quorum: {}",
                        key, total_ops, ok_count, l_quorum
                    );
                    // TODO: send actuall list of vdisk it has been written on
                    if ok_count >= l_quorum as usize {
                        ok2(SprinklerResult {
                            total_ops: total_ops as u16,
                            ok_ops: ok_count as u16,
                            quorum: l_quorum,
                        })
                    } else {
                        err2(SprinklerError {
                            total_ops: total_ops as u16,
                            ok_ops: ok_count as u16,
                            quorum: l_quorum,
                        })
                    }
                })
                .compat()
                .boxed()
        })
    }

    fn get_clustered(&self, key: BobKey) -> Get {
        Get({
            let target_nodes = self.calc_target_nodes(key);

            debug!(
                "GET[{}]: Nodes for fan out: {:?}",
                key,
                print_vec(&target_nodes)
            );
            let reqs = self
                .link_manager
                .call_nodes2(&target_nodes, |conn| conn.get(key).0.compat().boxed());

            let mut t = unordered::new();
            t = reqs.into_iter().collect();

            t
                .then(move |r| {
                    ok2::<_, ()>(r) // wrap all result kind to process it later
                })
                .filter(move |r| ready2(r.is_ok()))
                // .fold(vec![], |mut acc, r| {
                //     acc.push(r);
                //     ready2(acc)
                // })
                // .then(move |acc| {
                //     let finded_ok = acc.iter().find(|r| r.is_ok());
                //     match finded_ok {
                //         Some(q) => match q{
                //             Ok(q1) =>  match q1 {
                //                 Ok(q2) =>  ok2(*q2),   //TODO some kind of shit
                //                 _ => err2(BobError::NotFound),    
                //             }
                //             _ => err2(BobError::NotFound),
                //         },
                //         _ => err2(BobError::NotFound),
                //     }
                // })
                .compat()
                .boxed()
                
            // Box::new(
            //     select_ok(reqs) // any result will enought
            //         .map(|(r, _)| r)
            //         .map_err(|_r| BobError::NotFound),
            // )
        })
    }
}
