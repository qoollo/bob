use crate::api::grpc::{GetOptions, PutOptions};
use crate::core::{
    backend,
    backend::core::{BackendGetResult, BackendPutResult, Get, Put},
    cluster::Cluster,
    configs::node::NodeConfig,
    data::{print_vec, BobData, BobKey, ClusterResult, Node},
    link_manager::LinkManager,
    mapper::VDiskMapper,
};

use futures03::{
    future::{ready, FutureExt},
    stream::{FuturesUnordered, StreamExt},
};

use std::sync::Arc;

pub struct SimpleQuorumCluster {
    mapper: Arc<VDiskMapper>,
    quorum: u8,
}

impl SimpleQuorumCluster {
    pub fn new(mapper: Arc<VDiskMapper>, config: &NodeConfig) -> Self {
        SimpleQuorumCluster {
            quorum: config.quorum.unwrap(),
            mapper,
        }
    }

    fn calc_target_nodes(&self, key: BobKey) -> Vec<Node> {
        let target_vdisk = self.mapper.get_vdisk(key);
        target_vdisk.nodes.clone()
    }
}

impl Cluster for SimpleQuorumCluster {
    fn put_clustered_async(&self, key: BobKey, data: BobData) -> Put {
        let target_nodes = self.calc_target_nodes(key);

        debug!(
            "PUT[{}]: Nodes for fan out: {:?}",
            key,
            print_vec(&target_nodes)
        );

        let reqs = LinkManager::call_nodes(&target_nodes, |conn| {
            conn.put(
                key,
                &data,
                PutOptions {
                    remote_nodes: vec![], //TODO check
                    force_node: true,
                    overwrite: false,
                },
            )
            .0
        });

        let t = reqs.into_iter().collect::<FuturesUnordered<_>>();

        let l_quorum = self.quorum;
        let q = t
            .map(move |r| {
                trace!("PUT[{}] Response from cluster {:?}", key, r);
                r // wrap all result kind to process it later
            })
            .fold(vec![], |mut acc, r| {
                acc.push(r);
                ready(acc)
            })
            .map(move |acc| {
                debug!("PUT[{}] cluster ans: {:?}", key, acc);
                let total_ops = acc.iter().count();
                let mut sup = String::default();
                let ok_count = acc
                    .iter()
                    .filter(|&r| {
                        if let Err(e) = r {
                            sup = format!("{}, {:?}", sup.clone(), e)
                        }
                        r.is_ok()
                    })
                    .count();
                debug!(
                    "PUT[{}] total reqs: {} succ reqs: {} quorum: {}",
                    key, total_ops, ok_count, l_quorum
                );
                // TODO: send actuall list of vdisk it has been written on
                if ok_count >= l_quorum as usize {
                    Ok(BackendPutResult {})
                } else {
                    Err(backend::Error::Failed(format!(
                        "failed: total: {}, ok: {}, quorum: {}, sup: {}",
                        total_ops, ok_count, l_quorum, sup
                    )))
                }
            });
        Put(q.boxed())
    }

    fn get_clustered_async(&self, key: BobKey) -> Get {
        let target_nodes = self.calc_target_nodes(key);

        debug!(
            "GET[{}]: Nodes for fan out: {:?}",
            key,
            print_vec(&target_nodes)
        );
        let reqs = LinkManager::call_nodes(&target_nodes, |conn| {
            conn.get(key, GetOptions::new_normal()).0
        });

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
