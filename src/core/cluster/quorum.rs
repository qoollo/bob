use crate::core::{
    backend,
    backend::core::{BackendGetResult, BackendPutResult, Get, Put, Backend},
    configs::node::NodeConfig,
    data::{print_vec, BobData, BobKey, ClusterResult, Node, VDiskMapper},
    link_manager::LinkManager,
    cluster::Cluster,
};
use std::sync::Arc;

use futures03::{
    future::{ready, FutureExt},
    stream::{FuturesUnordered, StreamExt},
};

pub struct QuorumCluster {
    backend: Arc<Backend>,
    link_manager: Arc<LinkManager>,
    mapper: VDiskMapper,
    quorum: u8,
}

impl QuorumCluster {
    pub fn new(link_manager: Arc<LinkManager>, mapper: &VDiskMapper, config: &NodeConfig, backend: Arc<Backend>) -> Self {
        QuorumCluster {
            quorum: config.quorum.unwrap(),
            link_manager,
            mapper: mapper.clone(),
            backend,
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
        let l_quorum = self.quorum as usize;

        let target_nodes = self.calc_target_nodes(key);

        debug!(
            "PUT[{}]: Nodes for fan out: {:?}",
            key,
            print_vec(&target_nodes)
        );

        let reqs = self
            .link_manager
            .call_nodes(&target_nodes, |conn| conn.put(key, &data).0);
        let t = reqs.into_iter().collect::<FuturesUnordered<_>>();        
        let q = t
            .map(move |r| {
                trace!("PUT[{}] Response from cluster {:?}", key, r);
                r
            })
            .fold(vec![], |mut acc, r| {
                acc.push(r);
                ready(acc)
            })
            .map(move |acc| {
                debug!("PUT[{}] cluster ans: {:?}", key, acc);
                let total_ops = acc.iter().count();
                let mut failed = vec![];

                let ok_count = acc
                    .iter()
                    .filter(|&r| {
                        if let Err(e) = r {
                            failed.push(e);
                        }
                        r.is_ok()
                    })
                    .count();
                debug!(
                    "PUT[{}] total reqs: {} succ reqs: {} quorum: {}",
                    key, total_ops, ok_count, l_quorum
                );

                if ok_count == total_ops {
                    Ok(BackendPutResult {})
                }
                else if ok_count >= l_quorum {
                {                    
                    for _failed_node in failed {
                        // TODO write data locally in alien
                    }
                    Ok(BackendPutResult {})
                }
                } else {
                    // TODO write data locally and some other nodes
                    Err(backend::Error::Failed(format!(
                        "failed: total: {}, ok: {}, quorum: {}",
                        total_ops, ok_count, l_quorum
                    )))
                }
            });
        Put(q.boxed())
    }

    fn get_clustered(&self, key: BobKey) -> Get {
        let target_nodes = self.calc_target_nodes(key);

        debug!(
            "GET[{}]: Nodes for fan out: {:?}",
            key,
            print_vec(&target_nodes)
        );
        let reqs = self
            .link_manager
            .call_nodes(&target_nodes, |conn| conn.get(key).0);

        let t = reqs.into_iter().collect::<FuturesUnordered<_>>();

        let w = t
            .fold(vec![], |mut acc, r| {
                acc.push(r);
                ready(acc)
            })
            .map(move |acc| {
                let mut sup = String::default();
                acc.iter().for_each(|r| {
                    if let Err(e) = r {
                        trace!("GET[{}] failed result: {:?}", key, e);
                        sup = format!("{}, {:?}", sup.clone(), e)
                    } else if let Ok(e) = r {
                        trace!("GET[{}] success result from: {:?}", key, e.node);
                    }
                });

                let r = acc.into_iter().find(|r| r.is_ok());
                if let Some(answer) = r {
                    match answer {
                        Ok(ClusterResult { result: i, .. }) => Ok::<BackendGetResult, _>(i),
                        Err(ClusterResult { result: i, .. }) => Err::<_, backend::Error>(i),
                    }
                } else {
                    debug!("GET[{}] no success result", key);
                    Err::<_, backend::Error>(backend::Error::Failed(sup))
                }
            });
        Get(w.boxed())
    }
}