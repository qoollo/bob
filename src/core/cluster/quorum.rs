mod q_cluster {
    use crate::api::grpc::PutOptions;
    use crate::core::{
        backend,
        backend::core::{Backend, BackendGetResult, BackendOperation, BackendPutResult, Get, Put},
        cluster::Cluster,
        configs::node::NodeConfig,
        data::{print_vec, BobData, BobKey, ClusterResult, Node},
        link_manager::LinkManager,
        mapper::VDiskMapper,
    };
    use std::sync::Arc;

    use futures03::{
        future::{ready, FutureExt},
        stream::{FuturesUnordered, StreamExt},
    };

    use mockall::*;

    pub struct QuorumCluster {
        backend: Arc<Backend>,
        mapper: VDiskMapper,
        quorum: u8,
    }

    impl QuorumCluster {
        pub fn new(mapper: &VDiskMapper, config: &NodeConfig, backend: Arc<Backend>) -> Self {
            QuorumCluster {
                quorum: config.quorum.unwrap(),
                mapper: mapper.clone(),
                backend,
            }
        }

        pub(crate) fn calc_target_nodes(&self, key: BobKey) -> Vec<Node> {
            let target_vdisk = self.mapper.get_vdisk(key);

            target_vdisk.nodes.clone()
        }

        pub(crate) fn calc_sup_nodes(mapper: VDiskMapper, target_nodes: &[Node], count: usize) -> Vec<Node> {
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
                if indexes.iter().find(|&&i| i == cur_index as u16).is_some() {
                    trace!("node: {} is alrady target", nodes[cur_index]);
                    continue;
                }
                ret.push(nodes[cur_index].clone());
            }
            ret
        }

        async fn put_local_all(
            backend: Arc<Backend>,
            failed_nodes: Vec<Node>,
            key: BobKey,
            data: BobData,
            operation: BackendOperation,
        ) -> Result<(), PutOptions> {
            let mut add_nodes = vec![];
            for failed_node in failed_nodes.iter() {
                let mut op = operation.clone();
                op.set_remote_folder(&failed_node.name());

                let t = backend.put(&op, key, data.clone()).0.boxed().await;
                trace!("local support put result: {:?}", t);
                if t.is_err() {
                    add_nodes.push(failed_node.name());
                }
            }

            if add_nodes.len() > 0 {
                return Err(PutOptions::new_alien(&add_nodes));
            }
            Ok(())
        }

        async fn put_sup_nodes(
            key: BobKey,
            data: BobData,
            requests: &[(Node, PutOptions)],
        ) -> Result<(), (usize, String)> {
            let mut ret = vec![];
            for (node, options) in requests {
                let result =
                    LinkManager::call_node(&node, |conn| conn.put(key, &data, options.clone()).0)
                        .await;
                trace!("sup put to node: {}, result: {:?}", node, result);
                if let Err(e) = result {
                    ret.push(e);
                }
            }

            if ret.len() > 0 {
                return Err((
                    requests.len() - ret.len(),
                    ret.iter()
                        .fold("".to_string(), |acc, x| format!("{}, {:?}", acc, x)),
                ));
            }
            Ok(())
        }
    }

    impl Cluster for QuorumCluster {
        fn put_clustered(&self, key: BobKey, data: BobData) -> Put {
            let l_quorum = self.quorum as usize;
            let mapper = self.mapper.clone();
            let vdisk_id = self.mapper.get_vdisk(key).id.clone(); // remove search vdisk (not vdisk id)
            let backend = self.backend.clone();

            let target_nodes = self.calc_target_nodes(key);

            debug!(
                "PUT[{}]: Nodes for fan out: {:?}",
                key,
                print_vec(&target_nodes)
            );

            let reqs = LinkManager::call_nodes(&target_nodes, |conn| {
                conn.put(key, &data, PutOptions::new_client()).0
            })
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
                let failed: Vec<_> = acc
                    .into_iter()
                    .filter(|r| r.is_err())
                    .map(|e| e.err().unwrap())
                    .collect();
                let ok_count = total_ops - failed.len();

                debug!(
                    "PUT[{}] total reqs: {} succ reqs: {} quorum: {}",
                    key, total_ops, ok_count, l_quorum
                );
                if ok_count == total_ops {
                    Ok(BackendPutResult {})
                } else {
                    let mut additionl_remote_writes = match ok_count {
                        0 => l_quorum, //TODO take value from config
                        value if value < l_quorum => 1,
                        _ => 0,
                    };

                    let local_put = Self::put_local_all(
                        backend,
                        failed.iter().map(|n| n.node.clone()).collect(),
                        key,
                        data.clone(),
                        BackendOperation::new_alien(vdisk_id.clone()),
                    )
                    .await;

                    if local_put.is_err() {
                        additionl_remote_writes += 1;
                    }

                    let mut sup_nodes =
                        Self::calc_sup_nodes(mapper, &target_nodes, additionl_remote_writes);
                    debug!("sup put nodes: {}", print_vec(&sup_nodes));

                    let mut queries = vec![];

                    if let Err(op) = local_put {
                        let item = sup_nodes.remove(sup_nodes.len() - 1);
                        queries.push((item, op));
                    }

                    if additionl_remote_writes > 0 {
                        let nodes: Vec<String> =
                            failed.iter().map(|node| node.node.name()).collect();
                        let put_options = PutOptions::new_alien(&nodes);

                        let mut tt: Vec<_> = sup_nodes
                            .iter()
                            .map(|node| (node.clone(), put_options.clone()))
                            .collect();
                        queries.append(&mut tt);
                    }

                    if let Err((sup_ok_count, err)) = Self::put_sup_nodes(key, data, &queries).await
                    {
                        return if sup_ok_count + ok_count >= l_quorum {
                            Ok(BackendPutResult {})
                        } else {
                            Err(backend::Error::Failed(format!(
                                "failed: total: {}, ok: {}, quorum: {}, sup: {}",
                                total_ops,
                                ok_count + sup_ok_count,
                                l_quorum,
                                err
                            )))
                        };
                    }
                    Ok(BackendPutResult {})
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
            let reqs = LinkManager::call_nodes(&target_nodes, |conn| conn.get(key).0);

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

    mock!{
        pub QuorumCluster{
            fn new(mapper: &VDiskMapper, config: &NodeConfig, backend: Arc<Backend>) -> Self;
            fn calc_target_nodes(&self, key: BobKey) -> Vec<Node>;
        }
        pub trait Cluster {
            fn put_clustered(&self, key: BobKey, data: BobData) -> Put;
            fn get_clustered(&self, key: BobKey) -> Get;
        }
    }
}

cfg_if! {
    if #[cfg(test)] {
        pub use self::q_cluster::MockQuorumCluster as QuorumCluster;
    } else {
        pub use self::q_cluster::QuorumCluster;
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::core::{
        data::{BobKey, Node},
    };

    fn do_stuff(thing: QuorumCluster, key: BobKey) -> Vec<Node> {
        thing.calc_target_nodes(key)
    }

    #[test]
    fn some_test() {
        let mut mock = QuorumCluster::default();
        mock.expect_calc_target_nodes().returning(|key| vec![]);

        let key = BobKey::new(1);
        let t = do_stuff(mock, key);
        assert_eq!(0, t.len())
    }
}
