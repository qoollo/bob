use super::prelude::*;
use crate::core::backend::Exist;

pub(crate) struct Quorum {
    backend: Arc<Backend>,
    mapper: Arc<Virtual>,
    quorum: u8,
}

impl Quorum {
    pub(crate) fn new(mapper: Arc<Virtual>, config: &NodeConfig, backend: Arc<Backend>) -> Self {
        Self {
            quorum: config.quorum.expect("get quorum config"),
            mapper,
            backend,
        }
    }

    #[inline]
    pub(crate) fn get_target_nodes(&self, key: BobKey) -> Vec<Node> {
        self.mapper.get_vdisk_for_key(key).nodes().to_vec()
    }

    pub(crate) fn get_support_nodes(
        nodes: &[Node],
        target_indexes: &[u16],
        count: usize,
    ) -> Vec<Node> {
        if nodes.len() < target_indexes.len() + count {
            error!("cannot find enough support nodes"); //TODO check in mapper
            Vec::new()
        } else {
            nodes
                .iter()
                .filter(|node| target_indexes.iter().all(|&i| i != node.index()))
                .take(count)
                .cloned()
                .collect()
        }
    }

    async fn put_local_all(
        backend: Arc<Backend>,
        failed_nodes: Vec<String>,
        key: BobKey,
        data: BobData,
        operation: BackendOperation,
    ) -> Result<(), PutOptions> {
        let mut add_nodes = vec![];
        for failed_node in failed_nodes {
            let mut op = operation.clone();
            op.set_remote_folder(&failed_node);

            if let Err(e) = backend.put_local(key.clone(), data.clone(), op).await {
                debug!("PUT[{}] local support put result: {:?}", key, e);
                add_nodes.push(failed_node);
            }
        }

        if add_nodes.is_empty() {
            Ok(())
        } else {
            Err(PutOptions::new_alien(add_nodes))
        }
    }

    async fn put_sup_nodes(
        key: BobKey,
        data: BobData,
        requests: &[(Node, PutOptions)],
    ) -> Result<(), (usize, String)> {
        let mut ret = vec![];
        for (node, options) in requests {
            let result = LinkManager::call_node(&node, |client| {
                Box::pin(client.put(key, data.clone(), options.clone()))
            })
            .await;
            trace!(
                "PUT[{}] sup put to node: {:?}, result: {:?}",
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
    ) -> (Option<NodeOutput<BackendGetResult>>, String) {
        let sup = results
            .iter()
            .filter_map(|res| {
                trace!("GET[{}] failed result: {:?}", key, res);
                res.as_ref().err().map(|e| format!("{:?}", e))
            })
            .collect();
        let recent_successful = results
            .into_iter()
            .filter_map(Result::ok)
            .max_by_key(|r| r.inner().data.meta().timestamp());
        (recent_successful, sup)
    }

    async fn get_all(
        key: BobKey,
        target_nodes: &[Node],
        options: GetOptions,
    ) -> Vec<BobClientGetResult> {
        LinkManager::call_nodes(target_nodes, |conn| conn.get(key, options.clone()).0).await
    }

    fn group_keys_by_nodes(
        &self,
        keys: &[BobKey],
    ) -> HashMap<Vec<Node>, (Vec<BobKey>, Vec<usize>)> {
        let mut keys_by_nodes: HashMap<_, (Vec<_>, Vec<_>)> = HashMap::new();
        for (ind, &key) in keys.iter().enumerate() {
            keys_by_nodes
                .entry(self.get_target_nodes(key))
                .and_modify(|(keys, indexes)| {
                    keys.push(key);
                    indexes.push(ind);
                })
                .or_insert_with(|| (vec![key], vec![ind]));
        }
        keys_by_nodes
    }
}

impl Cluster for Quorum {
    //todo check duplicate data = > return error???
    fn put_clustered_async(&self, key: BobKey, data: BobData) -> BackendPut {
        let target_nodes = self.get_target_nodes(key);
        debug!("PUT[{}]: Nodes for fan out: {:?}", key, &target_nodes);

        let l_quorum = self.quorum as usize;
        let mapper = self.mapper.clone();
        let vdisk_id = self.mapper.get_vdisk_for_key(key).id(); // remove search vdisk (not vdisk id)
        let backend = self.backend.clone();
        let task = async move {
            let reqs = LinkManager::call_nodes(&target_nodes, |conn| {
                Box::pin(conn.put(key, data.clone(), PutOptions::new_client()))
            });
            let acc = reqs.await;
            debug!("PUT[{}] cluster ans: {:?}", key, acc);

            let total_ops = acc.len();
            debug!("total operations: {}", total_ops);
            debug!("all results: {:?}", acc);
            let failed = acc.into_iter().filter_map(Result::err).collect::<Vec<_>>();
            let ok_count = total_ops - failed.len();

            debug!(
                "PUT[{}] total reqs: {} succ reqs: {} quorum: {}",
                key, total_ops, ok_count, l_quorum
            );
            if ok_count == total_ops {
                Ok(())
            } else {
                let mut additionl_remote_writes = match ok_count {
                    0 => l_quorum, //TODO take value from config
                    value if value < l_quorum => 1,
                    _ => 0,
                };

                let local_put = Self::put_local_all(
                    backend,
                    failed.iter().map(|n| n.node_name().to_owned()).collect(),
                    key,
                    data.clone(),
                    BackendOperation::new_alien(vdisk_id),
                )
                .await;

                if local_put.is_err() {
                    additionl_remote_writes += 1;
                }

                let mut sup_nodes = Self::get_support_nodes(
                    mapper.nodes(),
                    &target_nodes
                        .iter()
                        .map(|node| node.index())
                        .collect::<Vec<_>>(),
                    additionl_remote_writes,
                );
                debug!("PUT[{}] sup put nodes: {:?}", key, &sup_nodes);

                let mut queries = Vec::new();

                if let Err(op) = local_put {
                    let item = sup_nodes.remove(sup_nodes.len() - 1);
                    queries.push((item, op));
                }

                if additionl_remote_writes > 0 {
                    let nodes = failed
                        .into_iter()
                        .map(|res| res.node_name().to_owned())
                        .collect::<Vec<_>>();
                    let put_options = PutOptions::new_alien(nodes);

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
                    Ok(())
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
        let all_nodes = self.get_target_nodes(key);

        let mapper = self.mapper.clone();
        let l_quorim = self.quorum as usize;
        let target_nodes = all_nodes.iter().take(l_quorim).cloned().collect::<Vec<_>>();

        debug!("GET[{}]: Nodes for fan out: {:?}", key, &target_nodes);

        let task = async move {
            let results = Self::get_all(key, &target_nodes, GetOptions::new_all()).await;
            debug!("GET[{}] cluster ans: {:?}", key, results);

            let (result, errors) = Self::get_filter_result(key, results); // @TODO refactoring of the error logs
            if let Some(answer) = result {
                debug!(
                    "GET[{}] take data from node: {}, timestamp: {}",
                    key,
                    answer.node_name(),
                    answer.inner().data.meta().timestamp()
                ); // TODO move meta
                return Ok(answer.into_inner());
            } else if errors.is_empty() {
                debug!("GET[{}] data not found", key);
                return Err(BackendError::KeyNotFound(key));
            }
            debug!("GET[{}] no success result", key);

            let mut sup_nodes = Self::get_support_nodes(
                mapper.nodes(),
                &all_nodes
                    .iter()
                    .map(|node| node.index())
                    .collect::<Vec<_>>(),
                1,
            ); // @TODO take from config
            sup_nodes.extend(all_nodes.into_iter().skip(l_quorim));

            debug!("GET[{}]: Sup nodes for fan out: {:?}", key, &sup_nodes);

            let second_attempt = Self::get_all(key, &sup_nodes, GetOptions::new_alien()).await;
            debug!("GET[{}] cluster ans sup: {:?}", key, second_attempt);

            let (result_sup, errors) = Self::get_filter_result(key, second_attempt);
            if let Some(answer) = result_sup {
                debug!(
                    "GET[{}] take data from node: {}, timestamp: {}",
                    key,
                    answer.node_name(),
                    answer.inner().data.meta().timestamp()
                ); // @TODO move meta
                Ok(answer.into_inner())
            } else {
                debug!("errors: {}", errors);
                debug!("GET[{}] data not found", key);
                Err(BackendError::KeyNotFound(key))
            }
        }
        .boxed();
        BackendGet(task)
    }

    fn exist_clustered_async(&self, keys: &[BobKey]) -> Exist {
        let keys_by_nodes = self.group_keys_by_nodes(keys);
        debug!(
            "EXIST Nodes for fan out: {:?}",
            &keys_by_nodes.keys().flatten().collect::<Vec<_>>()
        );
        let len = keys.len();
        Exist(
            async move {
                let mut exist = vec![false; len];
                for (nodes, (keys, indexes)) in keys_by_nodes {
                    let cluster_results = LinkManager::exist_on_nodes(&nodes, &keys).await;
                    for result in cluster_results {
                        if let Ok(result) = result {
                            for (&r, &ind) in result.inner().exist.iter().zip(&indexes) {
                                exist[ind] |= r;
                            }
                        }
                    }
                }
                Ok(BackendExistResult { exist })
            }
            .boxed(),
        )
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::core::cluster::Cluster;
    use crate::core::configs::cluster::tests::cluster_config;
    use crate::core::configs::{node::tests::node_config, node::NodeConfigYaml};
    use crate::core::configs::{ClusterConfig, ClusterConfigYaml, NodeConfig};
    use crate::core::{
        backend::Backend,
        bob_client::BobClient,
        data::{BobData, BobKey, BobMeta, Node, VDisk, VDiskId},
        mapper::Virtual,
    };
    use std::sync::Arc;
    use sup::*;

    mod sup {
        use crate::core::{bob_client::BobClient, data::Node, test_utils};
        use std::sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        };

        pub(crate) fn ping_ok(client: &mut BobClient, node: Node) {
            let cl = node;

            client
                .expect_ping()
                .returning(move || test_utils::ping_ok(cl.name().to_owned()));
        }

        pub(crate) fn put_ok(client: &mut BobClient, node: Node, call: Arc<CountCall>) {
            client.expect_put().returning(move |_key, _data, _options| {
                call.put_inc();
                test_utils::put_ok(node.name().to_owned())
            });
        }

        pub(crate) fn put_err(client: &mut BobClient, node: Node, call: Arc<CountCall>) {
            client.expect_put().returning(move |_key, _data, _options| {
                call.put_inc();
                test_utils::put_err(node.name().to_owned())
            });
        }

        pub(crate) fn get_ok_timestamp(
            client: &mut BobClient,
            node: Node,
            call: Arc<CountCall>,
            timestamp: i64,
        ) {
            client.expect_get().returning(move |_key, _options| {
                call.get_inc();
                test_utils::get_ok(node.name().to_owned(), timestamp)
            });
        }

        pub(crate) fn get_err(client: &mut BobClient, node: Node, call: Arc<CountCall>) {
            client.expect_get().returning(move |_key, _options| {
                call.get_inc();
                test_utils::get_err(node.name().to_owned())
            });
        }

        pub(crate) struct CountCall {
            put_count: AtomicU64,
            get_count: AtomicU64,
        }

        impl CountCall {
            pub(crate) fn new() -> Self {
                Self {
                    put_count: AtomicU64::new(0),
                    get_count: AtomicU64::new(0),
                }
            }

            pub(crate) fn put_inc(&self) {
                self.put_count.fetch_add(1, Ordering::SeqCst);
            }

            pub(crate) fn put_count(&self) -> u64 {
                self.put_count.load(Ordering::Relaxed)
            }

            pub(crate) fn get_inc(&self) {
                self.get_count.fetch_add(1, Ordering::SeqCst);
            }

            pub(crate) fn get_count(&self) -> u64 {
                self.get_count.load(Ordering::Relaxed)
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
        cluster: &ClusterConfig,
        map: &[(&str, Call, Arc<CountCall>)],
    ) -> (Quorum, Arc<Backend>) {
        let mapper = Arc::new(Virtual::new(vdisks, &node, &cluster));
        mapper.nodes().iter().for_each(|n| {
            let mut client = BobClient::default();
            let (_, func, call) = map
                .iter()
                .find(|(name, _, _)| *name == n.name())
                .expect("find node with name");
            func(&mut client, n.clone(), call.clone());

            n.set_connection(client);
        });

        let backend = Arc::new(Backend::new(mapper.clone(), &node));
        (Quorum::new(mapper, &node, backend.clone()), backend)
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
        let (quorum, backend) = create_cluster(vdisks, node, &cluster, &actions);

        let key = 1;
        let result = quorum
            .put_clustered_async(key, BobData::new(vec![], BobMeta::new(11)))
            .0
            .await;

        assert!(result.is_ok());
        assert_eq!(1, calls[0].1.put_count());
        let get = backend.get_local(key, BackendOperation::new_alien(0)).await;
        assert_eq!(backend::Error::KeyNotFound(key), get.err().unwrap());
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
        let (quorum, backend) = create_cluster(vdisks, node, &cluster, &actions);
        let key = 2;
        let result = quorum
            .put_clustered_async(key, BobData::new(vec![], BobMeta::new(11)))
            .0
            .await;

        assert!(result.is_ok());
        assert_eq!(1, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());

        let get = backend.get_local(key, BackendOperation::new_alien(0)).await;
        assert_eq!(backend::Error::KeyNotFound(key), get.err().unwrap());
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
        let (quorum, backend) = create_cluster(vdisks, node, &cluster, &actions);

        let mut result = quorum
            .put_clustered_async(3, BobData::new(vec![], BobMeta::new(11)))
            .0
            .await;

        assert!(result.is_ok());
        assert_eq!(0, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());

        result = quorum
            .put_clustered_async(4, BobData::new(vec![], BobMeta::new(11)))
            .0
            .await;

        assert!(result.is_ok());
        assert_eq!(1, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());
        let key = 3;
        let mut get = backend.get_local(key, BackendOperation::new_alien(0)).await;
        assert_eq!(backend::Error::KeyNotFound(key), get.err().unwrap());
        let key = 4;
        get = backend.get_local(key, BackendOperation::new_alien(0)).await;
        assert_eq!(backend::Error::KeyNotFound(key), get.err().unwrap());
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
        let (quorum, backend) = create_cluster(vdisks, node, &cluster, &actions);

        let result = quorum
            .put_clustered_async(5, BobData::new(vec![], BobMeta::new(11)))
            .0
            .await;

        assert!(result.is_err());
        assert_eq!(1, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());

        let get = backend.get_local(5, BackendOperation::new_alien(0)).await;
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
        let (quorum, backend) = create_cluster(vdisks, node, &cluster, &actions);

        let result = quorum
            .put_clustered_async(5, BobData::new(vec![], BobMeta::new(11)))
            .0
            .await;

        assert!(result.is_ok());
        assert_eq!(1, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());

        let get = backend.get_local(5, BackendOperation::new_alien(0)).await;
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
        let (quorum, backend) = create_cluster(vdisks, node, &cluster, &actions);

        let result = quorum
            .put_clustered_async(0, BobData::new(vec![], BobMeta::new(11)))
            .0
            .await;

        assert!(result.is_ok());
        assert_eq!(1, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());
        assert_eq!(1, calls[2].1.put_count());

        let get = backend.get_local(0, BackendOperation::new_alien(0)).await;
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
        let (quorum, backend) = create_cluster(vdisks, node, &cluster, &actions);

        let result = quorum
            .put_clustered_async(0, BobData::new(vec![], BobMeta::new(11)))
            .0
            .await;

        assert!(result.is_err());
        assert_eq!(1, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());
        assert_eq!(1, calls[2].1.put_count());

        let get = backend.get_local(0, BackendOperation::new_alien(0)).await;
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
        let (quorum, backend) = create_cluster(vdisks, node, &cluster, &actions);

        let result = quorum
            .put_clustered_async(0, BobData::new(vec![], BobMeta::new(11)))
            .0
            .await;

        assert!(result.is_ok());
        assert_eq!(1, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());
        assert_eq!(0, calls[2].1.put_count());

        let get = backend.get_local(0, BackendOperation::new_alien(0)).await;
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
        let (quorum, backend) = create_cluster(vdisks, node, &cluster, &actions);

        let result = quorum
            .put_clustered_async(0, BobData::new(vec![], BobMeta::new(11)))
            .0
            .await;

        assert!(result.is_ok());
        assert_eq!(1, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());
        assert_eq!(1, calls[2].1.put_count());

        let get = backend.get_local(0, BackendOperation::new_alien(0)).await;
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
        let (quorum, _) = create_cluster(vdisks, node, &cluster, &actions);

        let result = quorum.get_clustered_async(101).0.await;

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
        let (quorum, _) = create_cluster(vdisks, node, &cluster, &actions);

        let result = quorum.get_clustered_async(102).0.await;

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
        let (quorum, _) = create_cluster(vdisks, node, &cluster, &actions);

        let result = quorum.get_clustered_async(110).0.await;

        assert!(result.is_ok());
        assert_eq!(1, result.unwrap().data.meta().timestamp());
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
        let (quorum, _) = create_cluster(vdisks, node, &cluster, &actions);

        let result = quorum.get_clustered_async(110).0.await;

        assert!(result.is_ok());
        assert_eq!(1, result.unwrap().data.meta().timestamp());
        assert_eq!(1, calls[0].1.get_count());
    }
}
