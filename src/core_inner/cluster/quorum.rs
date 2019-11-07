use super::prelude::*;

pub struct QuorumCluster {
    backend: Arc<Backend>,
    mapper: Arc<VDiskMapper>,
    quorum: u8,
}

impl QuorumCluster {
    pub fn new(mapper: Arc<VDiskMapper>, config: &NodeConfig, backend: Arc<Backend>) -> Self {
        QuorumCluster {
            quorum: config.quorum.expect("get quorum config"),
            mapper,
            backend,
        }
    }

    pub(crate) fn calc_target_nodes(&self, key: BobKey) -> Vec<Node> {
        let target_vdisk = self.mapper.get_vdisk(key);
        target_vdisk.nodes.clone()
    }

    pub(crate) fn calc_sup_nodes(
        mapper: Arc<VDiskMapper>,
        target_nodes: &[Node],
        count: usize,
    ) -> Vec<Node> {
        let nodes = mapper.nodes();
        if nodes.len() > target_nodes.len() + count {
            error!("cannot find enough nodes for write"); //TODO check in mapper
            return vec![];
        }

        let indexes = target_nodes.iter().map(|n| n.index).collect::<Vec<_>>();
        trace!("target indexes: {:?}", indexes);

        let index_max = *indexes.iter().max().expect("get max index") as usize;

        let mut ret = vec![];
        let mut cur_index = index_max;
        let mut count_look = 0;
        while count_look < nodes.len() && ret.len() < count {
            cur_index = (cur_index + 1) % nodes.len();
            count_look += 1;

            //TODO check node is available
            if indexes.iter().any(|&i| i == cur_index as u16) {
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

            let t = backend.put_local(key, data.clone(), op).await;
            trace!("PUT[{}] local support put result: {:?}", key, t);
            if t.is_err() {
                add_nodes.push(failed_node.name());
            }
        }

        if add_nodes.is_empty() {
            Ok(())
        } else {
            Err(PutOptions::new_alien(&add_nodes))
        }
    }

    async fn put_sup_nodes(
        key: BobKey,
        data: BobData,
        requests: &[(Node, PutOptions)],
    ) -> Result<(), (usize, String)> {
        let mut ret = vec![];
        for (node, options) in requests {
            let result =
                LinkManager::call_node(&node, |mut conn| conn.put(key, &data, options.clone()).0)
                    .await;
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

        if ret.is_empty() {
            Ok(())
        } else {
            Err((
                requests.len() - ret.len(),
                ret.iter().map(|x| format!("{:?}", x)).collect(),
            ))
        }
    }

    fn get_filter_result(
        key: BobKey,
        results: Vec<BobClientGetResult>,
    ) -> (Option<ClusterResult<BackendGetResult>>, String) {
        let sup = results
            .iter()
            .filter_map(|res| {
                if let Err(e) = res {
                    trace!("GET[{}] failed result: {:?}", key, e);
                    Some(e.to_string())
                } else {
                    None
                }
            })
            .collect();

        (
            results
                .into_iter()
                .filter_map(|r| r.ok())
                .max_by_key(|r| r.result.data.meta.timestamp),
            sup,
        )
    }

    async fn get_all(
        key: BobKey,
        target_nodes: &[Node],
        options: GetOptions,
    ) -> Vec<BobClientGetResult> {
        LinkManager::call_nodes(target_nodes, |mut conn| conn.get(key, options.clone()).0)
            .into_iter()
            .collect::<FuturesUnordered<_>>()
            .collect()
            .await
    }
}

impl Cluster for QuorumCluster {
    //todo check duplicate data = > return error???
    fn put_clustered_async(&self, key: BobKey, data: BobData) -> BackendPut {
        let target_nodes = self.calc_target_nodes(key);
        debug!(
            "PUT[{}]: Nodes for fan out: {:?}",
            key,
            print_vec(&target_nodes)
        );

        let reqs = LinkManager::call_nodes(&target_nodes, |mut conn| {
            conn.put(key, &data, PutOptions::new_client()).0
        })
        .into_iter()
        .collect::<FuturesUnordered<_>>()
        .inspect(move |r| trace!("PUT[{}] Response from cluster {:?}", key, r))
        .collect::<Vec<_>>();

        let l_quorum = self.quorum as usize;
        let mapper = self.mapper.clone();
        let vdisk_id = self.mapper.get_vdisk(key).id.clone(); // remove search vdisk (not vdisk id)
        let backend = self.backend.clone();
        let task = async move {
            let acc = reqs.await;
            debug!("PUT[{}] cluster ans: {:?}", key, acc);

            let total_ops = acc.iter().count();
            let failed = acc.into_iter().filter_map(|r| r.err()).collect::<Vec<_>>();
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
                    failed.iter().map(|n| &n.node).cloned().collect(),
                    key,
                    data.clone(),
                    BackendOperation::new_alien(vdisk_id),
                )
                .await;

                if local_put.is_err() {
                    additionl_remote_writes += 1;
                }

                let mut sup_nodes =
                    Self::calc_sup_nodes(mapper, &target_nodes, additionl_remote_writes);
                debug!("PUT[{}] sup put nodes: {}", key, print_vec(&sup_nodes));

                let mut queries = Vec::new();

                if let Err(op) = local_put {
                    let item = sup_nodes.remove(sup_nodes.len() - 1);
                    queries.push((item, op));
                }

                if additionl_remote_writes > 0 {
                    let nodes = failed
                        .iter()
                        .map(|node| node.node.name())
                        .collect::<Vec<_>>();
                    let put_options = PutOptions::new_alien(&nodes);

                    queries.extend(
                        sup_nodes
                            .into_iter()
                            .map(|node| (node, put_options.clone())),
                    );
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
                    Err(BackendError::Failed(format!(
                        "failed: total: {}, ok: {}, quorum: {}, errors: {}",
                        total_ops,
                        ok_count + sup_ok_count,
                        l_quorum,
                        err
                    )))
                }
            }
        };
        BackendPut(task.boxed())
    }

    //todo check no data (no error)
    fn get_clustered_async(&self, key: BobKey) -> BackendGet {
        let all_nodes = self.calc_target_nodes(key);

        let mapper = self.mapper.clone();
        let l_quorim = self.quorum as usize;
        let target_nodes = all_nodes.iter().take(l_quorim).cloned().collect::<Vec<_>>();

        debug!(
            "GET[{}]: Nodes for fan out: {:?}",
            key,
            print_vec(&target_nodes)
        );

        let task = async move {
            let acc = Self::get_all(key, &target_nodes, GetOptions::new_all()).await;
            debug!("GET[{}] cluster ans: {:?}", key, acc);

            let (result, err) = Self::get_filter_result(key, acc);
            if let Some(answer) = result {
                debug!(
                    "GET[{}] take data from node: {}, timestamp: {}",
                    key, answer.node, answer.result.data.meta.timestamp
                ); // TODO move meta
                return Ok(answer.result);
            } else if err == "" {
                debug!("GET[{}] data not found", key);
                return Err::<_, backend::Error>(backend::Error::KeyNotFound);
            }
            debug!("GET[{}] no success result", key);

            let mut sup_nodes = Self::calc_sup_nodes(mapper, &all_nodes, 1); // TODO take from config
            sup_nodes.extend(all_nodes.into_iter().skip(l_quorim));

            debug!(
                "GET[{}]: Sup nodes for fan out: {:?}",
                key,
                print_vec(&sup_nodes)
            );

            let second_attemp = Self::get_all(key, &sup_nodes, GetOptions::new_alien()).await;
            debug!("GET[{}] cluster ans sup: {:?}", key, second_attemp);

            let (result_sup, err_sup) = Self::get_filter_result(key, second_attemp);
            if let Some(answer) = result_sup {
                debug!(
                    "GET[{}] take data from node: {}, timestamp: {}",
                    key, answer.node, answer.result.data.meta.timestamp
                ); // TODO move meta
                Ok(answer.result)
            } else {
                Err(BackendError::Failed(err + &err_sup))
            }
        }
        .boxed();
        BackendGet(task)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::core_inner::cluster::Cluster;
    use crate::core_inner::configs::cluster::tests::cluster_config;
    use crate::core_inner::configs::{node::tests::node_config, node::NodeConfigYaml};
    use crate::core_inner::configs::{ClusterConfig, ClusterConfigYaml, NodeConfig};
    use crate::core_inner::{
        backend::Backend,
        bob_client::BobClient,
        data::{BobData, BobKey, BobMeta, Node, VDisk, VDiskId},
        mapper::VDiskMapper,
    };
    use std::sync::Arc;
    use sup::*;

    mod sup {
        use crate::core_inner::{bob_client::tests, bob_client::BobClient, data::Node};
        use std::sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        };

        pub fn ping_ok(client: &mut BobClient, node: Node) {
            let cl = node;

            client
                .expect_ping()
                .returning(move || tests::ping_ok(cl.clone()));
        }

        pub fn put_ok(client: &mut BobClient, node: Node, call: Arc<CountCall>) {
            client.expect_put().returning(move |_key, _data, _options| {
                call.put_inc();
                tests::put_ok(node.clone())
            });
        }

        pub fn put_err(client: &mut BobClient, node: Node, call: Arc<CountCall>) {
            client.expect_put().returning(move |_key, _data, _options| {
                call.put_inc();
                tests::put_err(node.clone())
            });
        }

        pub fn get_ok_timestamp(
            client: &mut BobClient,
            node: Node,
            call: Arc<CountCall>,
            timestamp: i64,
        ) {
            client.expect_get().returning(move |_key, _options| {
                call.get_inc();
                tests::get_ok(node.clone(), timestamp)
            });
        }

        pub fn get_err(client: &mut BobClient, node: Node, call: Arc<CountCall>) {
            client.expect_get().returning(move |_key, _options| {
                call.get_inc();
                tests::get_err(node.clone())
            });
        }

        pub struct CountCall {
            put_count: AtomicU64,
            get_count: AtomicU64,
        }

        impl CountCall {
            pub fn new() -> Self {
                CountCall {
                    put_count: AtomicU64::new(0),
                    get_count: AtomicU64::new(0),
                }
            }

            pub fn put_inc(&self) {
                self.put_count.fetch_add(1, Ordering::SeqCst);
            }

            pub fn put_count(&self) -> u32 {
                self.put_count.load(Ordering::Relaxed) as u32
            }

            pub fn get_inc(&self) {
                self.get_count.fetch_add(1, Ordering::SeqCst);
            }

            pub fn get_count(&self) -> u32 {
                self.get_count.load(Ordering::Relaxed) as u32
            }
        }
    }

    fn prepare_configs(
        count_nodes: u8,
        count_vdisks: u8,
        count_replicas: u8,
        quorum: u8,
    ) -> (Vec<VDisk>, NodeConfig, ClusterConfig) {
        let node = node_config("0", quorum);
        let cluster = cluster_config(count_nodes, count_vdisks, count_replicas);
        NodeConfigYaml::check(&cluster, &node).expect("check node config");
        let vdisks = ClusterConfigYaml::convert(&cluster).expect("convert config");
        (vdisks, node, cluster)
    }

    fn create_cluster(
        vdisks: Vec<VDisk>,
        node: NodeConfig,
        cluster: ClusterConfig,
        map: Vec<(&str, Call, Arc<CountCall>)>,
    ) -> (QuorumCluster, Arc<Backend>) {
        let mapper = Arc::new(VDiskMapper::new(vdisks, &node, &cluster));
        mapper.nodes().iter().for_each(|n| {
            let mut client = BobClient::default();
            let (_, func, call) = map
                .iter()
                .find(|(name, _, _)| *name == n.name)
                .expect("find node with name");
            func(&mut client, n.clone(), call.clone());

            n.set_connection(client);
        });

        let backend = Arc::new(Backend::new(mapper.clone(), &node));
        (QuorumCluster::new(mapper, &node, backend.clone()), backend)
    }

    fn create_ok_node(name: &str, op: (bool, bool)) -> (&str, Call, Arc<CountCall>) {
        create_node(name, (op.0, op.1, 0))
    }

    fn create_node(name: &str, op: (bool, bool, i64)) -> (&str, Call, Arc<CountCall>) {
        (
            name,
            Box::new(
                move |client: &mut BobClient, n: Node, call: Arc<CountCall>| {
                    let f = |client: &mut BobClient,
                             n: Node,
                             c: Arc<CountCall>,
                             op: (bool, bool, i64)| {
                        ping_ok(client, n.clone());
                        if op.0 {
                            put_ok(client, n.clone(), c.clone());
                        } else {
                            put_err(client, n.clone(), c.clone());
                        }
                        if op.1 {
                            get_ok_timestamp(client, n, c, op.2);
                        } else {
                            get_err(client, n, c);
                        }
                    };
                    f(client, n.clone(), call.clone(), op);
                    client.expect_clone().returning(move || {
                        let mut cl = BobClient::default();
                        f(&mut cl, n.clone(), call.clone(), op);
                        cl
                    });
                },
            ),
            Arc::new(CountCall::new()),
        )
    }

    type Call = Box<dyn Fn(&mut BobClient, Node, Arc<CountCall>)>;

    //////////////////////////////////////////////////
    ////////////////////////////////////////////////// put
    //////////////////////////////////////////////////

    /// 1 node, 1 vdisk, 1 replics in vdisk, quorum = 1
    /// put data on node
    /// no data local
    #[tokio::test]
    async fn simple_one_node_put_ok() {
        // log4rs::init_file("./logger.yaml", Default::default()).unwrap();
        let (vdisks, node, cluster) = prepare_configs(1, 1, 1, 1);

        let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![create_ok_node("0", (true, true))];

        let calls: Vec<_> = actions
            .iter()
            .map(|(name, _, call)| ((*name).to_string(), call.clone()))
            .collect();
        let (quorum, backend) = create_cluster(vdisks, node, cluster, actions);

        let result = quorum
            .put_clustered_async(BobKey::new(1), BobData::new(vec![], BobMeta::new_value(11)))
            .0
            .await;

        assert!(result.is_ok());
        assert_eq!(1, calls[0].1.put_count());
        let get = backend
            .get_local(BobKey::new(1), BackendOperation::new_alien(VDiskId::new(0)))
            .await;
        assert_eq!(backend::Error::KeyNotFound, get.err().unwrap());
    }

    /// 2 node, 1 vdisk, 1 replics in vdisk, quorum = 1
    /// put data on both nodes
    /// no data local
    #[tokio::test]
    async fn simple_two_node_one_vdisk_cluster_put_ok() {
        let (vdisks, node, cluster) = prepare_configs(2, 1, 2, 1);

        let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
            create_ok_node("0", (true, true)),
            create_ok_node("1", (true, true)),
        ];

        let calls: Vec<_> = actions
            .iter()
            .map(|(name, _, call)| ((*name).to_string(), call.clone()))
            .collect();
        let (quorum, backend) = create_cluster(vdisks, node, cluster, actions);

        let result = quorum
            .put_clustered_async(BobKey::new(2), BobData::new(vec![], BobMeta::new_value(11)))
            .0
            .await;

        assert!(result.is_ok());
        assert_eq!(1, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());

        let get = backend
            .get_local(BobKey::new(2), BackendOperation::new_alien(VDiskId::new(0)))
            .await;
        assert_eq!(backend::Error::KeyNotFound, get.err().unwrap());
    }

    /// 2 node, 2 vdisk, 1 replics in vdisk, quorum = 1
    /// put first data to "1" node, check no data on "2" node
    /// put second data to "2" node, check one on "2" node
    /// no data local
    #[tokio::test]
    async fn simple_two_node_two_vdisk_one_replica_cluster_put_ok() {
        // log4rs::init_file("./logger.yaml", Default::default()).unwrap();
        let (vdisks, node, cluster) = prepare_configs(2, 2, 1, 1);

        let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
            create_ok_node("0", (true, true)),
            create_ok_node("1", (true, true)),
        ];

        let calls: Vec<_> = actions
            .iter()
            .map(|(name, _, call)| ((*name).to_string(), call.clone()))
            .collect();
        let (quorum, backend) = create_cluster(vdisks, node, cluster, actions);

        let mut result = quorum
            .put_clustered_async(BobKey::new(3), BobData::new(vec![], BobMeta::new_value(11)))
            .0
            .await;

        assert!(result.is_ok());
        assert_eq!(0, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());

        result = quorum
            .put_clustered_async(BobKey::new(4), BobData::new(vec![], BobMeta::new_value(11)))
            .0
            .await;

        assert!(result.is_ok());
        assert_eq!(1, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());

        let mut get = backend
            .get_local(BobKey::new(3), BackendOperation::new_alien(VDiskId::new(0)))
            .await;
        assert_eq!(backend::Error::KeyNotFound, get.err().unwrap());
        get = backend
            .get_local(BobKey::new(4), BackendOperation::new_alien(VDiskId::new(0)))
            .await;
        assert_eq!(backend::Error::KeyNotFound, get.err().unwrap());
    }

    /// 2 node, 1 vdisk, 2 replics in vdisk, quorum = 2
    /// one node failed => write one data local => no quorum => put err
    #[tokio::test]
    async fn two_node_one_vdisk_cluster_one_node_failed_put_err() {
        // log4rs::init_file("./logger.yaml", Default::default()).unwrap();
        let (vdisks, node, cluster) = prepare_configs(2, 1, 2, 2);
        // debug!("cluster: {:?}", cluster);
        let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
            create_ok_node("0", (true, true)),
            create_ok_node("1", (false, true)),
        ];

        let calls: Vec<_> = actions
            .iter()
            .map(|(name, _, call)| ((*name).to_string(), call.clone()))
            .collect();
        let (quorum, backend) = create_cluster(vdisks, node, cluster, actions);

        let result = quorum
            .put_clustered_async(BobKey::new(5), BobData::new(vec![], BobMeta::new_value(11)))
            .0
            .await;

        assert!(result.is_err());
        assert_eq!(1, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());

        let get = backend
            .get_local(BobKey::new(5), BackendOperation::new_alien(VDiskId::new(0)))
            .await;
        assert!(get.is_ok());
    }

    /// 2 node, 1 vdisk, 2 replics in vdisk, quorum = 1
    /// one node failed => write one data local => quorum => put ok
    #[tokio::test]
    async fn two_node_one_vdisk_cluster_one_node_failed_put_ok() {
        // log4rs::init_file("./logger.yaml", Default::default()).unwrap();
        let (vdisks, node, cluster) = prepare_configs(2, 1, 2, 1);
        // debug!("cluster: {:?}", cluster);
        let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
            create_ok_node("0", (true, true)),
            create_ok_node("1", (false, true)),
        ];

        let calls: Vec<_> = actions
            .iter()
            .map(|(name, _, call)| ((*name).to_string(), call.clone()))
            .collect();
        let (quorum, backend) = create_cluster(vdisks, node, cluster, actions);

        let result = quorum
            .put_clustered_async(BobKey::new(5), BobData::new(vec![], BobMeta::new_value(11)))
            .0
            .await;

        assert!(result.is_ok());
        assert_eq!(1, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());

        let get = backend
            .get_local(BobKey::new(5), BackendOperation::new_alien(VDiskId::new(0)))
            .await;
        assert!(get.is_ok());
    }

    /// 3 node, 2 vdisk, 2 replics in vdisk, quorum = 2
    /// one node failed => write one data local + one sup node => quorum => put ok
    #[tokio::test]
    async fn three_node_two_vdisk_cluster_one_node_failed_put_ok() {
        // log4rs::init_file("./logger.yaml", Default::default()).unwrap();
        let (vdisks, node, cluster) = prepare_configs(3, 2, 2, 2);
        // debug!("cluster: {:?}", cluster);
        let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
            create_ok_node("0", (true, true)),
            create_ok_node("1", (false, true)),
            create_ok_node("2", (true, true)),
        ];

        let calls: Vec<_> = actions
            .iter()
            .map(|(name, _, call)| ((*name).to_string(), call.clone()))
            .collect();
        let (quorum, backend) = create_cluster(vdisks, node, cluster, actions);

        let result = quorum
            .put_clustered_async(BobKey::new(0), BobData::new(vec![], BobMeta::new_value(11)))
            .0
            .await;

        assert!(result.is_ok());
        assert_eq!(1, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());
        assert_eq!(1, calls[2].1.put_count());

        let get = backend
            .get_local(BobKey::new(0), BackendOperation::new_alien(VDiskId::new(0)))
            .await;
        assert!(get.is_ok());
    }

    /// 3 node, 2 vdisk, 2 replics in vdisk, quorum = 2
    /// one node failed => write one data local + one sup node(failed) => quorum => put err
    #[tokio::test]
    async fn three_node_two_vdisk_cluster_one_node_failed_put_err() {
        // log4rs::init_file("./logger.yaml", Default::default()).unwrap();
        let (vdisks, node, cluster) = prepare_configs(3, 2, 2, 2);
        // debug!("cluster: {:?}", cluster);
        let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
            create_ok_node("0", (true, true)),
            create_ok_node("1", (false, true)),
            create_ok_node("2", (false, true)),
        ];

        let calls: Vec<_> = actions
            .iter()
            .map(|(name, _, call)| ((*name).to_string(), call.clone()))
            .collect();
        let (quorum, backend) = create_cluster(vdisks, node, cluster, actions);

        let result = quorum
            .put_clustered_async(BobKey::new(0), BobData::new(vec![], BobMeta::new_value(11)))
            .0
            .await;

        assert!(result.is_err());
        assert_eq!(1, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());
        assert_eq!(1, calls[2].1.put_count());

        let get = backend
            .get_local(BobKey::new(0), BackendOperation::new_alien(VDiskId::new(0)))
            .await;
        assert!(get.is_ok());
    }

    /// 3 node, 2 vdisk, 2 replics in vdisk, quorum = 2
    /// one node failed, but call other => quorum => put ok
    #[tokio::test]
    async fn three_node_two_vdisk_cluster_one_node_failed_put_ok2() {
        // log4rs::init_file("./logger.yaml", Default::default()).unwrap();
        let (vdisks, node, cluster) = prepare_configs(3, 2, 2, 2);
        // debug!("cluster: {:?}", cluster);
        let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
            create_ok_node("0", (true, true)),
            create_ok_node("1", (true, true)),
            create_ok_node("2", (false, true)),
        ];

        let calls: Vec<_> = actions
            .iter()
            .map(|(name, _, call)| ((*name).to_string(), call.clone()))
            .collect();
        let (quorum, backend) = create_cluster(vdisks, node, cluster, actions);

        let result = quorum
            .put_clustered_async(BobKey::new(0), BobData::new(vec![], BobMeta::new_value(11)))
            .0
            .await;

        assert!(result.is_ok());
        assert_eq!(1, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());
        assert_eq!(0, calls[2].1.put_count());

        let get = backend
            .get_local(BobKey::new(0), BackendOperation::new_alien(VDiskId::new(0)))
            .await;
        assert!(get.is_err());
    }

    /// 3 node, 1 vdisk, 3 replics in vdisk, quorum = 2
    /// one node failed => local write => quorum => put ok
    #[tokio::test]
    async fn three_node_one_vdisk_cluster_one_node_failed_put_ok() {
        // log4rs::init_file("./logger.yaml", Default::default()).unwrap();
        let (vdisks, node, cluster) = prepare_configs(3, 1, 3, 2);
        // debug!("cluster: {:?}", cluster);
        let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
            create_ok_node("0", (true, true)),
            create_ok_node("1", (true, true)),
            create_ok_node("2", (false, true)),
        ];

        let calls: Vec<_> = actions
            .iter()
            .map(|(name, _, call)| ((*name).to_string(), call.clone()))
            .collect();
        let (quorum, backend) = create_cluster(vdisks, node, cluster, actions);

        let result = quorum
            .put_clustered_async(BobKey::new(0), BobData::new(vec![], BobMeta::new_value(11)))
            .0
            .await;

        assert!(result.is_ok());
        assert_eq!(1, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());
        assert_eq!(1, calls[2].1.put_count());

        let get = backend
            .get_local(BobKey::new(0), BackendOperation::new_alien(VDiskId::new(0)))
            .await;
        assert!(get.is_ok());
    }

    //////////////////////////////////////////////////
    ////////////////////////////////////////////////// get
    //////////////////////////////////////////////////

    /// 1 node, 1 vdisk, 1 replics in vdisk, quorum = 1
    /// get data => ok
    #[tokio::test]
    async fn simple_one_node_get_ok() {
        // log4rs::init_file("./logger.yaml", Default::default()).unwrap();
        let (vdisks, node, cluster) = prepare_configs(1, 1, 1, 1);

        let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![create_ok_node("0", (true, true))];

        let calls: Vec<_> = actions
            .iter()
            .map(|(name, _, call)| ((*name).to_string(), call.clone()))
            .collect();
        let (quorum, _) = create_cluster(vdisks, node, cluster, actions);

        let result = quorum.get_clustered_async(BobKey::new(101)).0.await;

        assert!(result.is_ok());
        assert_eq!(1, calls[0].1.get_count());
    }

    /// 1 node, 1 vdisk, 1 replics in vdisk, quorum = 1
    /// get no data => err
    #[tokio::test]
    async fn simple_one_node_get_err() {
        // log4rs::init_file("./logger.yaml", Default::default()).unwrap();
        let (vdisks, node, cluster) = prepare_configs(1, 1, 1, 1);

        let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![create_ok_node("0", (true, false))];

        let calls: Vec<_> = actions
            .iter()
            .map(|(name, _, call)| ((*name).to_string(), call.clone()))
            .collect();
        let (quorum, _) = create_cluster(vdisks, node, cluster, actions);

        let result = quorum.get_clustered_async(BobKey::new(102)).0.await;

        assert!(result.is_err());
        assert_eq!(1, calls[0].1.get_count());
    }

    /// 2 nodes, 1 vdisk, 2 replics in vdisk, quorum = 2
    /// get data from 2 nodes => get differ timetsmps => get max => ok
    #[tokio::test]
    async fn simple_two_node_get_ok() {
        // log4rs::init_file("./logger.yaml", Default::default()).unwrap();
        let (vdisks, node, cluster) = prepare_configs(2, 1, 2, 2);

        let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
            create_node("0", (true, true, 0)),
            create_node("1", (true, true, 1)),
        ];

        let calls: Vec<_> = actions
            .iter()
            .map(|(name, _, call)| ((*name).to_string(), call.clone()))
            .collect();
        let (quorum, _) = create_cluster(vdisks, node, cluster, actions);

        let result = quorum.get_clustered_async(BobKey::new(110)).0.await;

        assert!(result.is_ok());
        assert_eq!(1, result.unwrap().data.meta.timestamp);
        assert_eq!(1, calls[0].1.get_count());
    }

    /// 2 nodes, 2 vdisk, 1 replics in vdisk, quorum = 1
    /// get data from 1 nodes => fail => read from sup node => ok
    #[tokio::test]
    async fn simple_two_node_read_sup_get_ok() {
        // log4rs::init_file("./logger.yaml", Default::default()).unwrap();
        let (vdisks, node, cluster) = prepare_configs(2, 2, 1, 1);

        let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
            create_node("0", (true, false, 0)),
            create_node("1", (true, true, 1)),
        ];

        let calls: Vec<_> = actions
            .iter()
            .map(|(name, _, call)| ((*name).to_string(), call.clone()))
            .collect();
        let (quorum, _) = create_cluster(vdisks, node, cluster, actions);

        let result = quorum.get_clustered_async(BobKey::new(110)).0.await;

        assert!(result.is_ok());
        assert_eq!(1, result.unwrap().data.meta.timestamp);
        assert_eq!(1, calls[0].1.get_count());
    }
}
