use crate::core::{
    backend,
    backend::core::{BackendGetResult, BackendPutResult, Get, Put, PutResult, Backend, BackendOperation},
    configs::node::NodeConfig,
    bob_client::BobClient,
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

    fn calc_sup_nodes(mapper: VDiskMapper, target_nodes: &[Node], count: usize) -> Vec<Node>{
        let nodes = mapper.nodes();
        if nodes.len() > target_nodes.len() + count {
            error!("cannot find enough nodes for write"); //TODO check in mapper
            return vec![];
        }
        
        let indexes: Vec<u16> = target_nodes.iter().map(|n| n.index).collect();
        let index_max = *indexes.iter().max().unwrap() as usize;

        let mut ret = vec![];
        let mut cur_index = index_max;
        let mut count_look = 0;
        while count_look < nodes.len() || ret.len() < count {
            cur_index = (cur_index + 1) % nodes.len();
            count_look += 1;

            //TODO check node is available
            if indexes.iter().find(|&&i|i == cur_index as u16).is_some() {
                trace!("node: {} is alrady target", nodes[cur_index]);
                continue;
            }
            ret.push(nodes[cur_index].clone());
        }
        ret
    }

    async fn put_local(backend: Arc<Backend>, key: BobKey, data: BobData, op: BackendOperation) -> PutResult {
        backend.put(&op, key, data).0.boxed().await
    }
}

impl Cluster for QuorumCluster {
    fn put_clustered(&self, key: BobKey, data: BobData) -> Put {
        let l_quorum = self.quorum as usize;
        let mapper = self.mapper.clone();
        let vdisk_id = self.mapper.get_vdisk(key).id.clone(); // remove search vdisk (not vdisk id)
        let backend = self.backend.clone();
        let link = self.link_manager.clone();

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
                let mut additionl_remote_writes = match ok_count {
                    0 => l_quorum,                      //TODO take value from config
                    value if value < l_quorum => 1,
                    _ => 0,

                };
                
                let mut add_nodes = vec![];
                for failed_node in &failed {
                    let mut op = BackendOperation::new_alien(vdisk_id.clone());
                    op.set_remote_folder(&failed_node.node.name());
                    
                    let t = Self::put_local(backend.clone(), key, data.clone(), op).await;
                    if t.is_err()
                    {
                        add_nodes.push(failed_node.node.name());
                    }
                }
                if add_nodes.len()>0 {
                    additionl_remote_writes += 1;
                }

                let mut sup_nodes = Self::calc_sup_nodes(mapper, &target_nodes, additionl_remote_writes);
                let mut queries = vec![];

                if add_nodes.len()>0 {
                    let item = sup_nodes.remove(sup_nodes.len() - 1);

                    queries.push((item, |conn: &mut BobClient| conn.put(key, &data, PutOptions::new_alien(&add_nodes)).0));
                }

                if additionl_remote_writes > 0 {
                    let nodes : Vec<String>= failed.iter().map(|node| node.node.name()).collect();
                    let put_options = PutOptions::new_alien(&nodes);
                    
                    // for node in sup_nodes {
                    //     queries.push((node, |conn: &mut BobClient| conn.put(key, &data, PutOptions::new_alien(&add_nodes)).0));
                    // }
                    // let mut tt: Vec<_> = sup_nodes
                    //     .iter()
                    //     .map(|node| (node.clone(), |conn: &mut BobClient| conn.put(key, &data, put_options.clone()).0))
                    //     .collect();
                    // queries.append(&mut tt);
                }
                let call = link.call_nodes_direct(queries);
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