use crate::core::{
    backend::backend::{BackendError, BackendPutResult, Get, Put},
    configs::node::NodeConfig,
    data::{print_vec, BobData, BobKey, Node, VDiskMapper},
    link_manager::LinkManager,
};
use std::sync::Arc;

use futures03::{
    future::{err, ok, ready, FutureExt},
    stream::{FuturesUnordered, StreamExt},
};

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

        let l_quorum = self.quorum;
        let q = t
            .then(move |r| {
                trace!("PUT[{}] Response from cluster {:?}", key, r);
                ok::<_, ()>(r) // wrap all result kind to process it later
            })
            .fold(vec![], |mut acc, r| {
                acc.push(r);
                ready(acc)
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
                    ok(BackendPutResult {})
                } else {
                    err(BackendError::Failed(format!(
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

        let mut w = t.skip_while(move |r| ready(!r.is_ok()));
        let q = async move {
            w.next()
                .map(|r| {
                    r.map(|res| res.map(|ok| ok.result).map_err(|err| err.result))
                        .unwrap()
                }) // TODO handle errors
                .await
        };
        Get(q.boxed())
    }
}
