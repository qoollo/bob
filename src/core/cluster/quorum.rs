use crate::api::grpc::PutOptions;
use crate::core::{
    backend,
    backend::core::{Backend, BackendGetResult, BackendOperation, BackendPutResult, Get, Put},
    cluster::Cluster,
    configs::node::NodeConfig,
    data::{print_vec, BobData, BobKey, ClusterResult, Node},
    link_manager::LinkManager,
    mapper::VDiskMapper,
    bob_client::GetResult,
};
use std::sync::Arc;

use futures03::{
    future::{ready, FutureExt},
    stream::{FuturesUnordered, StreamExt},
};

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

    pub(crate) fn calc_sup_nodes(
        mapper: VDiskMapper,
        target_nodes: &[Node],
        count: usize,
    ) -> Vec<Node> {
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
        while count_look < nodes.len() && ret.len() < count {
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
            trace!("PUT[{}] local support put result: {:?}", key, t);
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
                LinkManager::call_node(&node, |conn| conn.put(key, &data, options.clone()).0).await;
            trace!(
                "PUT[{}] sup put to node: {}, result: {:?}",
                key,
                node,
                result
            );
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

    fn get_filter_result(key: BobKey, results: Vec<GetResult>)
        -> (Option<ClusterResult<BackendGetResult>>, String)
    {
        let mut sup = String::default();
        results.iter().for_each(|r| {
            if let Err(e) = r {
                trace!("GET[{}] failed result: {:?}", key, e);
                sup = format!("{}, {:?}", sup.clone(), e)
            } else if let Ok(e) = r {
                trace!("GET[{}] success result from: {:?}", key, e.node);
            }
        });

        (results.into_iter().filter_map(|r| r.ok()).max_by_key(|r|r.result.data.meta.timestamp), sup)
    }

    async fn get_all(key: BobKey, target_nodes: Vec<Node>) -> Vec<GetResult> {
        LinkManager::call_nodes(&target_nodes, |conn| conn.get(key).0)
            .into_iter()
            .collect::<FuturesUnordered<_>>()
            .fold(vec![], |mut acc, r| {
                acc.push(r);
                ready(acc)
            })
            .boxed()
            .await
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
                debug!("PUT[{}] sup put nodes: {}", key, print_vec(&sup_nodes));

                let mut queries = vec![];

                if let Err(op) = local_put {
                    let item = sup_nodes.remove(sup_nodes.len() - 1);
                    queries.push((item, op));
                }

                if additionl_remote_writes > 0 {
                    let nodes: Vec<String> = failed.iter().map(|node| node.node.name()).collect();
                    let put_options = PutOptions::new_alien(&nodes);

                    let mut tt: Vec<_> = sup_nodes
                        .iter()
                        .map(|node| (node.clone(), put_options.clone()))
                        .collect();
                    queries.append(&mut tt);
                }

                let mut sup_ok_count = queries.len();
                let mut err = String::new();

                if let Err((sup_ok_count_l, err_l)) = Self::put_sup_nodes(key, data, &queries).await
                {
                    sup_ok_count = sup_ok_count_l;
                    err = err_l;
                }
                trace!(
                    "PUT[{}] sup_ok: {}, ok_count: {}, quorum: {}, errors: {}",
                    key,
                    sup_ok_count,
                    ok_count,
                    l_quorum,
                    err
                );
                if sup_ok_count + ok_count >= l_quorum {
                    Ok(BackendPutResult {})
                } else {
                    Err(backend::Error::Failed(format!(
                        "failed: total: {}, ok: {}, quorum: {}, errors: {}",
                        total_ops,
                        ok_count + sup_ok_count,
                        l_quorum,
                        err
                    )))
                }
            }
        };
        Put(p.boxed())
    }

    fn get_clustered(&self, key: BobKey) -> Get {
        let mapper = self.mapper.clone();
        let l_quorim = self.quorum as usize;

        let all_nodes: Vec<_> = self.calc_target_nodes(key);

        let target_nodes: Vec<_> = all_nodes
            .iter()
            .take(l_quorim)
            .map(|n| n.clone())
            .collect();
        
        debug!(
            "GET[{}]: Nodes for fan out: {:?}",
            key,
            print_vec(&target_nodes)
        );

        let g = async move{
            let acc = Self::get_all(key, target_nodes).await;
            debug!("GET[{}] cluster ans: {:?}", key, acc);

            let (result, err) = Self::get_filter_result(key, acc);
            if let Some(answer) = result {
                debug!("GET[{}] take data from node: {}, timestamp: {}", key, answer.node, answer.result.data.meta.timestamp); // TODO move meta
                return Ok(answer.result);
            }
            debug!("GET[{}] no success result", key);

            let mut sup_nodes = Self::calc_sup_nodes(mapper, &all_nodes, 1); // TODO take from config
            sup_nodes.append(&mut all_nodes.into_iter().skip(l_quorim).collect());

            debug!(
                "GET[{}]: Sup nodes for fan out: {:?}",
                key,
                print_vec(&sup_nodes)
            );

            let second_attemp = Self::get_all(key, sup_nodes).await;
            debug!("GET[{}] cluster ans sup: {:?}", key, second_attemp);

            let (result_sup, err_sup) = Self::get_filter_result(key, second_attemp);
            if let Some(answer) = result_sup {
                debug!("GET[{}] take data from node: {}, timestamp: {}", key, answer.node, answer.result.data.meta.timestamp); // TODO move meta
                return Ok(answer.result);
            }

            Err::<_, backend::Error>(backend::Error::Failed(err + &err_sup))
        };
        Get(g.boxed())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::core::cluster::Cluster;
    use crate::core::{
        backend::core::Backend,
        bob_client::BobClient,
        configs::{
            cluster::{
                ClusterConfig, ClusterConfigYaml, Node as ConfigNode, NodeDisk, Replica,
                VDisk as VDiskConfig,
            },
            node::{NodeConfig, NodeConfigYaml},
        },
        data::{BobData, BobKey, BobMeta, Node, VDisk, VDiskId},
        mapper::VDiskMapper,
    };
    use log4rs;

    use futures03::executor::{ThreadPool, ThreadPoolBuilder};
    use std::cell::{Cell, RefCell};
    use std::sync::Arc;
    use sup::*;

    mod sup {
        use crate::core::{bob_client::tests, bob_client::BobClient, data::Node};
        use std::sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        };

        pub fn ping_ok(client: &mut BobClient, node: Node) {
            let cl = node.clone();

            client
                .expect_ping()
                .returning(move || tests::ping_ok(cl.clone()));
        }

        pub fn put_ok(client: &mut BobClient, node: Node, call: Arc<CountCall>) {
            let cl = node.clone();
            client.expect_put().returning(move |_key, _data, _options| {
                call.put_inc();
                tests::put_ok(cl.clone())
            });
        }

        pub fn put_err(client: &mut BobClient, node: Node, call: Arc<CountCall>) {
            let cl = node.clone();
            client.expect_put().returning(move |_key, _data, _options| {
                call.put_inc();
                tests::put_err(cl.clone())
            });
        }

        pub struct CountCall {
            put_count: AtomicU64,
        }
        impl CountCall {
            pub fn new() -> Self {
                CountCall {
                    put_count: AtomicU64::new(0),
                }
            }
            pub fn put_inc(&self) {
                self.put_count.fetch_add(1, Ordering::SeqCst);
            }
            pub fn put_count(&self) -> u32 {
                self.put_count.load(Ordering::Relaxed) as u32
            }
        }
    }

    fn get_pool() -> ThreadPool {
        ThreadPoolBuilder::new().pool_size(1).create().unwrap()
    }

    fn node_config(quorum: u8) -> NodeConfig {
        let config = NodeConfig {
            log_config: Some("".to_string()),
            name: Some("0".to_string()),
            quorum: Some(quorum),
            timeout: Some("3sec".to_string()),
            check_interval: Some("3sec".to_string()),
            cluster_policy: Some("quorum".to_string()),
            ping_threads_count: Some(4),
            grpc_buffer_bound: Some(4),
            backend_type: Some("in_memory".to_string()),
            pearl: None,
            metrics: None,
            bind_ref: RefCell::default(),
            timeout_ref: Cell::default(),
            check_ref: Cell::default(),
            disks_ref: RefCell::default(),
        };
        config
    }

    fn cluster_config(count_nodes: u8, count_vdisks: u8, count_replicas: u8) -> ClusterConfig {
        let nodes = (0..count_nodes)
            .map(|id| {
                let name = id.to_string();
                ConfigNode {
                    name: Some(name.clone()),
                    address: Some("1".to_string()),
                    disks: vec![NodeDisk {
                        name: Some(name.clone()),
                        path: Some(name.clone()),
                    }],
                    host: RefCell::default(),
                    port: Cell::default(),
                }
            })
            .collect();

        let vdisks = (0..count_vdisks)
            .map(|id| {
                let replicas = (0..count_replicas)
                    .map(|r| {
                        let n = ((id + r) % count_nodes).to_string();
                        Replica {
                            node: Some(n.clone()),
                            disk: Some(n.clone()),
                        }
                    })
                    .collect();
                VDiskConfig {
                    id: Some(id as i32),
                    replicas,
                }
            })
            .collect();

        let config = ClusterConfig { nodes, vdisks };
        config
    }

    fn prepare_configs(
        count_nodes: u8,
        count_vdisks: u8,
        count_replicas: u8,
        quorum: u8,
    ) -> (Vec<VDisk>, NodeConfig, ClusterConfig) {
        let node = node_config(quorum);
        let cluster = cluster_config(count_nodes, count_vdisks, count_replicas);
        NodeConfigYaml::check(&cluster, &node).unwrap();
        let vdisks = ClusterConfigYaml::convert(&cluster).unwrap();
        (vdisks, node, cluster)
    }

    fn create_cluster(
        pool: &ThreadPool,
        vdisks: Vec<VDisk>,
        node: NodeConfig,
        cluster: ClusterConfig,
        map: Vec<(&str, Call, Arc<CountCall>)>,
    ) -> (QuorumCluster, Arc<Backend>) {
        let mapper = VDiskMapper::new(vdisks, &node, &cluster);
        mapper.nodes().iter().for_each(|n| {
            let mut client = BobClient::default();
            let (_, func, call) = map
                .iter()
                .find(|(name, _, _)| name.to_string() == n.name)
                .unwrap();
            func(&mut client, n.clone(), call.clone());

            n.set_connection(client);
        });

        let backend = Arc::new(Backend::new(&mapper, &node, pool.clone()));
        (
            QuorumCluster::new(&mapper, &node, backend.clone()),
            backend.clone(),
        )
    }

    fn create_ok_node(name: &str, op: (bool, bool)) -> (&str, Call, Arc<CountCall>) {
        (
            name,
            Box::new(
                move |client: &mut BobClient, n: Node, call: Arc<CountCall>| {
                    let f =
                        |client: &mut BobClient, n: Node, c: Arc<CountCall>, op: (bool, bool)| {
                            ping_ok(client, n.clone());
                            if op.0 {
                                put_ok(client, n.clone(), c);
                            } else {
                                put_err(client, n.clone(), c);
                            }
                        };
                    f(client, n.clone(), call.clone(), op.clone());
                    client.expect_clone().returning(move || {
                        let mut cl = BobClient::default();
                        f(&mut cl, n.clone(), call.clone(), op.clone());
                        cl
                    });
                },
            ),
            Arc::new(CountCall::new()),
        )
    }

    type Call = Box<dyn Fn(&mut BobClient, Node, Arc<CountCall>)>;

    /// 1 node, 1 vdisk, 1 replics in vdisk, quorum = 1
    /// put data on node
    /// no data local
    #[test]
    fn simple_one_node_put_ok() {
        // log4rs::init_file("./logger.yaml", Default::default()).unwrap();
        let mut pool = get_pool();
        let (vdisks, node, cluster) = prepare_configs(1, 1, 1, 1);

        let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![create_ok_node("0", (true, true))];

        let calls: Vec<_> = actions
            .iter()
            .map(|(name, _, call)| (name.clone(), call.clone()))
            .collect();
        let (quorum, backend) = create_cluster(&pool, vdisks, node, cluster, actions);

        let result = pool.run(
            quorum
                .put_clustered(BobKey::new(1), BobData::new(vec![], BobMeta::new_value(11)))
                .0,
        );

        assert!(result.is_ok());
        assert_eq!(1, calls[0].1.put_count());
        let get = pool.run(
            backend
                .get(
                    &BackendOperation::new_alien(VDiskId::new(0)),
                    BobKey::new(1),
                )
                .0,
        );
        assert_eq!(backend::Error::KeyNotFound, get.err().unwrap());
    }

    /// 2 node, 1 vdisk, 1 replics in vdisk, quorum = 1
    /// put data on both nodes
    /// no data local
    #[test]
    fn simple_two_node_one_vdisk_cluster_put_ok() {
        // log4rs::init_file("./logger.yaml", Default::default()).unwrap();
        let mut pool = get_pool();
        let (vdisks, node, cluster) = prepare_configs(2, 1, 2, 1);

        let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
            create_ok_node("0", (true, true)),
            create_ok_node("1", (true, true)),
        ];

        let calls: Vec<_> = actions
            .iter()
            .map(|(name, _, call)| (name.clone(), call.clone()))
            .collect();
        let (quorum, backend) = create_cluster(&pool, vdisks, node, cluster, actions);

        let result = pool.run(
            quorum
                .put_clustered(BobKey::new(2), BobData::new(vec![], BobMeta::new_value(11)))
                .0,
        );

        assert!(result.is_ok());
        assert_eq!(1, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());

        let get = pool.run(
            backend
                .get(
                    &BackendOperation::new_alien(VDiskId::new(0)),
                    BobKey::new(2),
                )
                .0,
        );
        assert_eq!(backend::Error::KeyNotFound, get.err().unwrap());
    }

    /// 2 node, 2 vdisk, 1 replics in vdisk, quorum = 1
    /// put first data to "1" node, check no data on "2" node
    /// put second data to "2" node, check one on "2" node
    /// no data local
    #[test]
    fn simple_two_node_two_vdisk_one_replica_cluster_put_ok() {
        // log4rs::init_file("./logger.yaml", Default::default()).unwrap();
        let mut pool = get_pool();
        let (vdisks, node, cluster) = prepare_configs(2, 2, 1, 1);

        let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
            create_ok_node("0", (true, true)),
            create_ok_node("1", (true, true)),
        ];

        let calls: Vec<_> = actions
            .iter()
            .map(|(name, _, call)| (name.clone(), call.clone()))
            .collect();
        let (quorum, backend) = create_cluster(&pool, vdisks, node, cluster, actions);

        let mut result = pool.run(
            quorum
                .put_clustered(BobKey::new(3), BobData::new(vec![], BobMeta::new_value(11)))
                .0,
        );

        assert!(result.is_ok());
        assert_eq!(0, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());

        result = pool.run(
            quorum
                .put_clustered(BobKey::new(4), BobData::new(vec![], BobMeta::new_value(11)))
                .0,
        );

        assert!(result.is_ok());
        assert_eq!(1, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());

        let mut get = pool.run(
            backend
                .get(
                    &BackendOperation::new_alien(VDiskId::new(0)),
                    BobKey::new(3),
                )
                .0,
        );
        assert_eq!(backend::Error::KeyNotFound, get.err().unwrap());
        get = pool.run(
            backend
                .get(
                    &BackendOperation::new_alien(VDiskId::new(0)),
                    BobKey::new(4),
                )
                .0,
        );
        assert_eq!(backend::Error::KeyNotFound, get.err().unwrap());
    }

    /// 2 node, 1 vdisk, 2 replics in vdisk, quorum = 2
    /// one node failed => write one data local => no quorum => put err
    #[test]
    fn two_node_one_vdisk_cluster_one_node_failed_put_err() {
        // log4rs::init_file("./logger.yaml", Default::default()).unwrap();
        let mut pool = get_pool();
        let (vdisks, node, cluster) = prepare_configs(2, 1, 2, 2);
        // debug!("cluster: {:?}", cluster);
        let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
            create_ok_node("0", (true, true)),
            create_ok_node("1", (false, true)),
        ];

        let calls: Vec<_> = actions
            .iter()
            .map(|(name, _, call)| (name.clone(), call.clone()))
            .collect();
        let (quorum, backend) = create_cluster(&pool, vdisks, node, cluster, actions);

        let result = pool.run(
            quorum
                .put_clustered(BobKey::new(5), BobData::new(vec![], BobMeta::new_value(11)))
                .0,
        );

        assert!(result.is_err());
        assert_eq!(1, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());

        let get = pool.run(
            backend
                .get(
                    &BackendOperation::new_alien(VDiskId::new(0)),
                    BobKey::new(5),
                )
                .0,
        );
        assert!(get.is_ok());
    }

    /// 2 node, 1 vdisk, 2 replics in vdisk, quorum = 1
    /// one node failed => write one data local => quorum => put ok
    #[test]
    fn two_node_one_vdisk_cluster_one_node_failed_put_ok() {
        // log4rs::init_file("./logger.yaml", Default::default()).unwrap();
        let mut pool = get_pool();
        let (vdisks, node, cluster) = prepare_configs(2, 1, 2, 1);
        // debug!("cluster: {:?}", cluster);
        let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
            create_ok_node("0", (true, true)),
            create_ok_node("1", (false, true)),
        ];

        let calls: Vec<_> = actions
            .iter()
            .map(|(name, _, call)| (name.clone(), call.clone()))
            .collect();
        let (quorum, backend) = create_cluster(&pool, vdisks, node, cluster, actions);

        let result = pool.run(
            quorum
                .put_clustered(BobKey::new(5), BobData::new(vec![], BobMeta::new_value(11)))
                .0,
        );

        assert!(result.is_ok());
        assert_eq!(1, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());

        let get = pool.run(
            backend
                .get(
                    &BackendOperation::new_alien(VDiskId::new(0)),
                    BobKey::new(5),
                )
                .0,
        );
        assert!(get.is_ok());
    }

    /// 3 node, 2 vdisk, 2 replics in vdisk, quorum = 2
    /// one node failed => write one data local + one sup node => quorum => put ok
    #[test]
    fn three_node_two_vdisk_cluster_one_node_failed_put_ok() {
        // log4rs::init_file("./logger.yaml", Default::default()).unwrap();
        let mut pool = get_pool();
        let (vdisks, node, cluster) = prepare_configs(3, 2, 2, 2);
        // debug!("cluster: {:?}", cluster);
        let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
            create_ok_node("0", (true, true)),
            create_ok_node("1", (false, true)),
            create_ok_node("2", (true, true)),
        ];

        let calls: Vec<_> = actions
            .iter()
            .map(|(name, _, call)| (name.clone(), call.clone()))
            .collect();
        let (quorum, backend) = create_cluster(&pool, vdisks, node, cluster, actions);

        let result = pool.run(
            quorum
                .put_clustered(BobKey::new(0), BobData::new(vec![], BobMeta::new_value(11)))
                .0,
        );

        assert!(result.is_ok());
        assert_eq!(1, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());
        assert_eq!(1, calls[2].1.put_count());

        let get = pool.run(
            backend
                .get(
                    &BackendOperation::new_alien(VDiskId::new(0)),
                    BobKey::new(0),
                )
                .0,
        );
        assert!(get.is_ok());
    }

    /// 3 node, 2 vdisk, 2 replics in vdisk, quorum = 2
    /// one node failed => write one data local + one sup node(failed) => quorum => put err
    #[test]
    fn three_node_two_vdisk_cluster_one_node_failed_put_err() {
        // log4rs::init_file("./logger.yaml", Default::default()).unwrap();
        let mut pool = get_pool();
        let (vdisks, node, cluster) = prepare_configs(3, 2, 2, 2);
        // debug!("cluster: {:?}", cluster);
        let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
            create_ok_node("0", (true, true)),
            create_ok_node("1", (false, true)),
            create_ok_node("2", (false, true)),
        ];

        let calls: Vec<_> = actions
            .iter()
            .map(|(name, _, call)| (name.clone(), call.clone()))
            .collect();
        let (quorum, backend) = create_cluster(&pool, vdisks, node, cluster, actions);

        let result = pool.run(
            quorum
                .put_clustered(BobKey::new(0), BobData::new(vec![], BobMeta::new_value(11)))
                .0,
        );

        assert!(result.is_err());
        assert_eq!(1, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());
        assert_eq!(1, calls[2].1.put_count());

        let get = pool.run(
            backend
                .get(
                    &BackendOperation::new_alien(VDiskId::new(0)),
                    BobKey::new(0),
                )
                .0,
        );
        assert!(get.is_ok());
    }

    /// 3 node, 2 vdisk, 2 replics in vdisk, quorum = 2
    /// one node failed, but call other => quorum => put ok
    #[test]
    fn three_node_two_vdisk_cluster_one_node_failed_put_ok2() {
        // log4rs::init_file("./logger.yaml", Default::default()).unwrap();
        let mut pool = get_pool();
        let (vdisks, node, cluster) = prepare_configs(3, 2, 2, 2);
        // debug!("cluster: {:?}", cluster);
        let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
            create_ok_node("0", (true, true)),
            create_ok_node("1", (true, true)),
            create_ok_node("2", (false, true)),
        ];

        let calls: Vec<_> = actions
            .iter()
            .map(|(name, _, call)| (name.clone(), call.clone()))
            .collect();
        let (quorum, backend) = create_cluster(&pool, vdisks, node, cluster, actions);

        let result = pool.run(
            quorum
                .put_clustered(BobKey::new(0), BobData::new(vec![], BobMeta::new_value(11)))
                .0,
        );

        assert!(result.is_ok());
        assert_eq!(1, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());
        assert_eq!(0, calls[2].1.put_count());

        let get = pool.run(
            backend
                .get(
                    &BackendOperation::new_alien(VDiskId::new(0)),
                    BobKey::new(0),
                )
                .0,
        );
        assert!(get.is_err());
    }

    /// 3 node, 1 vdisk, 3 replics in vdisk, quorum = 2
    /// one node failed => local write => quorum => put ok
    #[test]
    fn three_node_one_vdisk_cluster_one_node_failed_put_ok() {
        // log4rs::init_file("./logger.yaml", Default::default()).unwrap();
        let mut pool = get_pool();
        let (vdisks, node, cluster) = prepare_configs(3, 1, 3, 2);
        // debug!("cluster: {:?}", cluster);
        let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
            create_ok_node("0", (true, true)),
            create_ok_node("1", (true, true)),
            create_ok_node("2", (false, true)),
        ];

        let calls: Vec<_> = actions
            .iter()
            .map(|(name, _, call)| (name.clone(), call.clone()))
            .collect();
        let (quorum, backend) = create_cluster(&pool, vdisks, node, cluster, actions);

        let result = pool.run(
            quorum
                .put_clustered(BobKey::new(0), BobData::new(vec![], BobMeta::new_value(11)))
                .0,
        );

        assert!(result.is_ok());
        assert_eq!(1, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());
        assert_eq!(1, calls[2].1.put_count());

        let get = pool.run(
            backend
                .get(
                    &BackendOperation::new_alien(VDiskId::new(0)),
                    BobKey::new(0),
                )
                .0,
        );
        assert!(get.is_ok());
    }
}
