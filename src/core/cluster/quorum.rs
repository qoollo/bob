use crate::core::{
    backend,
    backend::core::{BackendGetResult, BackendPutResult, Get, Put, PutResult, Backend, BackendOperation},
    configs::node::NodeConfig,
    data::{print_vec, BobData, BobKey, ClusterResult, Node},
    mapper::VDiskMapper,
    link_manager::LinkManager,
    cluster::Cluster,
};
use crate::api::grpc::PutOptions;
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

        target_vdisk
            .nodes
            .clone()
    }

    // #[allow(dead_code)]
    // fn calc_sup_nodes(target_nodes: &[Node], count: u8) -> Result<Vec<Node>, String>{
    //     let mut node = target_nodes[target_nodes.len()-1].get_next_node();

    //     let mut retry = target_nodes.len();
    //     while target_nodes.iter().find(|n| n.name == node.name).is_some() && retry > 0 {
    //         node = node.get_next_node();
    //         retry -= 1;
    //     }
    //     if retry == 0 {
    //         debug!("replics count == count nodes. cannot take sup nodes"); // TODO make this check after start
    //         return Err("replics count == count nodes. cannot take sup nodes".to_string());
    //     }

    //     let mut result = vec![];
    //     for _ in 0..count {
    //         //TODO check nodes are alive
    //         if  target_nodes.iter().find(|n| n.name == node.name).is_none() {
    //             result.push(node.clone());
    //         }
    //         else {
    //             error!("cannot find {} sup node", count);
    //             return Err(format!("cannot find {} sup node", count));
    //         }
    //         node = node.get_next_node();
    //     }

    //     // Ok(result)
    //     unimplemented!();
    // }

    async fn put_local(backend: Arc<Backend>, key: BobKey, data: BobData, op: BackendOperation) -> PutResult {
        backend.put(&op, key, data).0.boxed().await
    }
}

impl Cluster for QuorumCluster {
    fn put_clustered(&self, key: BobKey, data: BobData) -> Put {
        let l_quorum = self.quorum as usize;
        let vdisk_id = self.mapper.get_vdisk(key).id.clone(); // remove search vdisk (not vdisk id)
        let backend = self.backend.clone();

        let target_nodes = self.calc_target_nodes(key);

        debug!(
            "PUT[{}]: Nodes for fan out: {:?}",
            key,
            print_vec(&target_nodes)
        );

        let reqs = self
            .link_manager
            .call_nodes(&target_nodes, |conn| conn.put(key, &data,  PutOptions::new_client()).0)
            .into_iter()
            .collect::<FuturesUnordered<_>>()
            .map(move |r| {
                trace!("PUT[{}] Response from cluster {:?}", key, r);
                r
            })
            .fold(vec![], |mut acc, r| {
                acc.push(r);
                ready(acc)
            });
        
        let p = async move {
            let acc = reqs.boxed().await;
            debug!("PUT[{}] cluster ans: {:?}", key, acc);

            let total_ops = acc.iter().count();
            let failed: Vec<_>= acc
                .into_iter()
                .filter(|r| r.is_err())
                .map(|e|e.err().unwrap())
                .collect();
            let ok_count = total_ops - failed.len();
            
            debug!(
                "PUT[{}] total reqs: {} succ reqs: {} quorum: {}",
                key, total_ops, ok_count, l_quorum
            );
            if ok_count == total_ops {
                Ok(BackendPutResult {})
            }
             else {
                // let mut additionl_remote_writes = match ok_count {
                //     0 => l_quorum,                      //TODO take value from config
                //     value if value < l_quorum => 1,
                //     _ => 0,

                // };
                
                // let mut local_fail = false;
                for failed_node in failed {
                    let mut op = BackendOperation::new_alien(vdisk_id.clone());
                    op.set_remote_folder(&failed_node.node.name);
                    
                    let t = Self::put_local(backend.clone(), key, data.clone(), op).await;
                    // if t.is_err()
                    // {
                    //     local_fail = true; // TODO write only this data
                    // }
                }
                // if local_fail {
                //     additionl_remote_writes += 1;
                // }

                Ok(BackendPutResult {})

                // TODO write data locally and some other nodes
                // Err(backend::Error::Failed(format!(
                //     "failed: total: {}, ok: {}, quorum: {}",
                //     total_ops, ok_count, l_quorum
                // )))
            }
        };
        Put(p.boxed())
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