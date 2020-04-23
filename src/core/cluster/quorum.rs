use super::prelude::*;

pub(crate) struct Quorum {
    backend: Arc<Backend>,
    mapper: Arc<Virtual>,
    quorum: usize,
}

impl Quorum {
    pub(crate) fn new(backend: Arc<Backend>, mapper: Arc<Virtual>, quorum: usize) -> Self {
        Self {
            backend,
            mapper,
            quorum,
        }
    }

    #[inline]
    fn get_target_nodes(&self, key: BobKey) -> &[Node] {
        self.mapper.get_vdisk_for_key(key).nodes()
    }

    fn get_support_nodes<'a>(
        &'a self,
        mut target_indexes: impl Iterator<Item = u16>,
        count: usize,
    ) -> Result<Vec<&'a Node>, BackendError> {
        let (len, _) = target_indexes.size_hint();
        debug!("iterator size lower bound: {}", len);
        trace!("nodes available: {}", self.mapper.nodes().len());
        if self.mapper.nodes().len() < len + count {
            let msg = "cannot find enough support nodes".to_owned();
            error!("{}", msg);
            Err(BackendError::Failed(msg))
        } else {
            let sup = self
                .mapper
                .nodes()
                .iter()
                .filter(|node| target_indexes.all(|i| i != node.index()))
                .take(count)
                .collect();
            Ok(sup)
        }
    }

    async fn put_local_all(
        &self,
        node_names: Vec<String>,
        key: BobKey,
        data: BobData,
        operation: BackendOperation,
    ) -> Result<(), PutOptions> {
        let mut add_nodes = vec![];
        for node_name in node_names {
            let mut op = operation.clone();
            op.set_remote_folder(node_name.clone());

            if let Err(e) = self.backend.put_local(key, data.clone(), op).await {
                debug!("PUT[{}] local support put result: {:?}", key, e);
                add_nodes.push(node_name);
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
        requests: &[(&Node, PutOptions)],
    ) -> Result<(), (usize, String)> {
        let mut ret = vec![];
        for (node, options) in requests {
            let result = LinkManager::call_node(&node, |client| {
                Box::pin(client.put(key, data.clone(), options.clone()))
            })
            .await;
            if let Err(e) = result {
                ret.push(e);
            }
        }

        if ret.is_empty() {
            Ok(())
        } else {
            let msg = ret.iter().map(|x| format!("{:?}\n", x)).collect();
            Err((requests.len() - ret.len(), msg))
        }
    }

    fn filter_get_results(
        key: BobKey,
        results: Vec<BobClientGetResult>,
    ) -> (Option<NodeOutput<BobData>>, String) {
        let sup = results
            .iter()
            .filter_map(|res| {
                trace!("GET[{}] received result: {:?}", key, res);
                res.as_ref().err().map(|e| format!("{:?}", e))
            })
            .collect();
        let recent_successful = results
            .into_iter()
            .filter_map(Result::ok)
            .max_by_key(NodeOutput::timestamp);
        (recent_successful, sup)
    }

    async fn get_all(
        key: BobKey,
        target_nodes: &[Node],
        options: GetOptions,
    ) -> Vec<BobClientGetResult> {
        LinkManager::call_nodes(target_nodes, |conn| conn.get(key, options.clone()).boxed()).await
    }

    fn group_keys_by_nodes(
        &self,
        keys: &[BobKey],
    ) -> HashMap<Vec<Node>, (Vec<BobKey>, Vec<usize>)> {
        let mut keys_by_nodes: HashMap<_, (Vec<_>, Vec<_>)> = HashMap::new();
        for (ind, &key) in keys.iter().enumerate() {
            keys_by_nodes
                .entry(self.get_target_nodes(key).to_vec())
                .and_modify(|(keys, indexes)| {
                    keys.push(key);
                    indexes.push(ind);
                })
                .or_insert_with(|| (vec![key], vec![ind]));
        }
        keys_by_nodes
    }
}

#[async_trait]
impl Cluster for Quorum {
    async fn put(&self, key: BobKey, data: BobData) -> PutResult {
        debug!("get nodes of the target vdisk");
        let target_nodes = self.get_target_nodes(key);
        debug!("PUT[{}]: Nodes for fan out: {:?}", key, &target_nodes);
        debug!("call put on target nodes (on target vdisk)");
        let results = LinkManager::call_nodes(&target_nodes, |conn| {
            Box::pin(conn.put(key, data.clone(), PutOptions::new_local()))
        })
        .await;
        debug!("PUT[{}] rcv {} cluster answers", key, results.len());

        let total_ops = results.len();
        debug!("filter out ok results");
        let errors = results
            .into_iter()
            .filter_map(Result::err)
            .collect::<Vec<_>>();
        let ok_count = total_ops - errors.len();

        debug!("ok: {}/{} quorum: {}", ok_count, total_ops, self.quorum);
        if ok_count == total_ops {
            Ok(())
        } else {
            let mut additional_remote_writes = match ok_count {
                0 => self.quorum, //@TODO take value from config
                value if value < self.quorum => 1,
                _ => 0,
            };

            let vdisk_id = self.mapper.id_from_key(key);
            debug!("get names of the failed nodes");
            let node_names = errors.iter().map(|n| n.node_name().to_owned()).collect();
            debug!("create operation Backend alien");
            let operation = BackendOperation::new_alien(vdisk_id);
            let local_put = self
                .put_local_all(node_names, key, data.clone(), operation)
                .await;

            if local_put.is_err() {
                debug!("local put failed, add another remote node");
                additional_remote_writes += 1;
            }
            let target_indexes = target_nodes.iter().map(Node::index);
            let mut sup_nodes = self.get_support_nodes(target_indexes, additional_remote_writes)?;
            debug!("PUT[{}] sup put nodes: {:?}", key, &sup_nodes);

            let mut queries = Vec::new();

            if let Err(op) = local_put {
                let item = sup_nodes.remove(sup_nodes.len() - 1);
                queries.push((item, op));
            }

            if additional_remote_writes > 0 {
                let nodes = errors
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

            if let Err((sup_ok_count_l, err_l)) = Self::put_sup_nodes(key, data, &queries).await {
                sup_ok_count = sup_ok_count_l;
                err = err_l;
            }
            if sup_ok_count + ok_count >= self.quorum {
                Ok(())
            } else {
                let msg = format!(
                    "failed: total: {}, ok: {}, quorum: {}, errors: {}",
                    total_ops,
                    ok_count + sup_ok_count,
                    self.quorum,
                    err
                );
                let e = BackendError::Failed(msg);
                Err(e)
            }
        }
    }

    //todo check no data (no error)
    async fn get(&self, key: BobKey) -> GetResult {
        let all_nodes = self.get_target_nodes(key);
        trace!("target nodes: {:?}", all_nodes);
        debug!("GET[{}]: Nodes for fan out: {:?}", key, &all_nodes);

        // @TODO currently we send requests to all nodes, ignoring quorum value.
        // In future we need to call local node in first place and then remaining nodes.
        let results = Self::get_all(key, all_nodes, GetOptions::new_all()).await;
        debug!("GET[{}] cluster ans: {:?}", key, results);

        let (result, errors) = Self::filter_get_results(key, results);
        if let Some(answer) = result {
            debug!(
                "GET[{}] take data from node: {}, timestamp: {}",
                key,
                answer.node_name(),
                answer.timestamp()
            ); // TODO move meta
            return Ok(answer.into_inner());
        } else if errors.is_empty() {
            debug!("GET[{}] data not found", key);
            return Err(BackendError::KeyNotFound(key));
        }
        trace!("appropriate nodes didn't get successful results, lookup in other nodes aliens");
        debug!("GET[{}] no success result", key);

        let sup_nodes: Vec<_> = self
            .get_support_nodes(all_nodes.iter().map(Node::index), 0)?
            .into_iter()
            .cloned()
            .collect(); // @TODO take from config

        debug!("GET[{}]: Sup nodes for fan out: {:?}", key, &sup_nodes);

        let second_attempt = Self::get_all(key, &sup_nodes, GetOptions::new_alien()).await;
        debug!("GET[{}] cluster ans sup: {:?}", key, second_attempt);

        let (result_sup, errors) = Self::filter_get_results(key, second_attempt);
        if let Some(answer) = result_sup {
            debug!(
                "GET[{}] take data from node: {}, timestamp: {}",
                key,
                answer.node_name(),
                answer.timestamp()
            );
            Ok(answer.into_inner())
        } else {
            debug!("errors: {}", errors);
            debug!("GET[{}] data not found", key);
            Err(BackendError::KeyNotFound(key))
        }
    }

    async fn exist(&self, keys: &[BobKey]) -> ExistResult {
        let keys_by_nodes = self.group_keys_by_nodes(keys);
        debug!(
            "EXIST Nodes for fan out: {:?}",
            &keys_by_nodes.keys().flatten().collect::<Vec<_>>()
        );
        let len = keys.len();
        let mut exist = vec![false; len];
        for (nodes, (keys, indexes)) in keys_by_nodes {
            let cluster_results = LinkManager::exist_on_nodes(&nodes, &keys).await;
            for result in cluster_results {
                if let Ok(result) = result {
                    for (&r, &ind) in result.inner().iter().zip(&indexes) {
                        exist[ind] |= r;
                    }
                }
            }
        }
        Ok(exist)
    }
}

#[cfg(test)]
mod tests {
    use super::super::prelude::*;
    use crate::core::configs::{
        cluster::tests::cluster_config, node::tests::node_config, Cluster as ClusterConfig,
    };
    use std::sync::atomic::{AtomicU64, Ordering};

    fn ping_ok(client: &mut BobClient, node: Node) {
        let cl = node;

        client
            .expect_ping()
            .returning(move || test_utils::ping_ok(cl.name().to_owned()));
    }

    fn put_ok(client: &mut BobClient, node: Node, call: Arc<CountCall>) {
        client.expect_put().returning(move |_key, _data, _options| {
            call.put_inc();
            test_utils::put_ok(node.name().to_owned())
        });
    }

    fn put_err(client: &mut BobClient, node: Node, call: Arc<CountCall>) {
        client.expect_put().returning(move |_key, _data, _options| {
            call.put_inc();
            test_utils::put_err(node.name().to_owned())
        });
    }

    fn get_ok_timestamp(client: &mut BobClient, node: Node, call: Arc<CountCall>, timestamp: u64) {
        client.expect_get().returning(move |_key, _options| {
            call.get_inc();
            test_utils::get_ok(node.name().to_owned(), timestamp)
        });
    }

    fn get_err(client: &mut BobClient, node: Node, call: Arc<CountCall>) {
        client.expect_get().returning(move |_key, _options| {
            call.get_inc();
            test_utils::get_err(node.name().to_owned())
        });
    }

    struct CountCall {
        put_count: AtomicU64,
        get_count: AtomicU64,
    }

    impl CountCall {
        fn new() -> Self {
            Self {
                put_count: AtomicU64::new(0),
                get_count: AtomicU64::new(0),
            }
        }

        fn put_inc(&self) {
            self.put_count.fetch_add(1, Ordering::SeqCst);
        }

        fn put_count(&self) -> u64 {
            self.put_count.load(Ordering::Relaxed)
        }

        fn get_inc(&self) {
            self.get_count.fetch_add(1, Ordering::SeqCst);
        }

        fn get_count(&self) -> u64 {
            self.get_count.load(Ordering::Relaxed)
        }
    }

    fn prepare_configs(
        count_nodes: u32,
        count_vdisks: u32,
        count_replicas: u32,
        quorum: usize,
    ) -> (Vec<VDisk>, NodeConfig, ClusterConfig) {
        let node = node_config("0", quorum);
        let cluster = cluster_config(count_nodes, count_vdisks, count_replicas);
        cluster.check(&node).expect("check node config");
        let vdisks = cluster.convert().expect("convert config");
        (vdisks, node, cluster)
    }

    async fn create_cluster(
        vdisks: Vec<VDisk>,
        node: &NodeConfig,
        cluster: &ClusterConfig,
        map: &[(&str, Call, Arc<CountCall>)],
    ) -> (Quorum, Arc<Backend>) {
        let mapper = Arc::new(Virtual::new(vdisks, &node, &cluster).await);
        for node in mapper.nodes().iter() {
            let mut client = BobClient::default();
            let (_, func, call) = map
                .iter()
                .find(|(name, _, _)| *name == node.name())
                .expect("find node with name");
            func(&mut client, node.clone(), call.clone());

            node.set_connection(client).await;
        }

        let backend = Arc::new(Backend::new(mapper.clone(), &node));
        (Quorum::new(backend.clone(), mapper, node.quorum()), backend)
    }

    fn create_ok_node(name: &str, op: (bool, bool)) -> (&str, Call, Arc<CountCall>) {
        create_node(name, (op.0, op.1, 0))
    }

    fn create_node(name: &str, op: (bool, bool, u64)) -> (&str, Call, Arc<CountCall>) {
        (
            name,
            Box::new(
                move |client: &mut BobClient, n: Node, call: Arc<CountCall>| {
                    let f = |client: &mut BobClient,
                             n: Node,
                             c: Arc<CountCall>,
                             op: (bool, bool, u64)| {
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
        let (quorum, backend) = create_cluster(vdisks, &node, &cluster, &actions).await;

        let key = 1;
        let result = quorum
            .put(key, BobData::new(vec![], BobMeta::new(11)))
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
        let (quorum, backend) = create_cluster(vdisks, &node, &cluster, &actions).await;
        let key = 2;
        let result = quorum
            .put(key, BobData::new(vec![], BobMeta::new(11)))
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
        let (quorum, backend) = create_cluster(vdisks, &node, &cluster, &actions).await;

        let mut result = quorum.put(3, BobData::new(vec![], BobMeta::new(11))).await;

        assert!(result.is_ok());
        assert_eq!(0, calls[0].1.put_count());
        assert_eq!(1, calls[1].1.put_count());

        result = quorum.put(4, BobData::new(vec![], BobMeta::new(11))).await;

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
        let (quorum, backend) = create_cluster(vdisks, &node, &cluster, &actions).await;

        let result = quorum.put(5, BobData::new(vec![], BobMeta::new(11))).await;

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
        let (quorum, backend) = create_cluster(vdisks, &node, &cluster, &actions).await;

        let result = quorum.put(5, BobData::new(vec![], BobMeta::new(11))).await;

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
        let (quorum, backend) = create_cluster(vdisks, &node, &cluster, &actions).await;

        let result = quorum.put(0, BobData::new(vec![], BobMeta::new(11))).await;

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
        let (quorum, backend) = create_cluster(vdisks, &node, &cluster, &actions).await;

        let result = quorum.put(0, BobData::new(vec![], BobMeta::new(11))).await;

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
        let (quorum, backend) = create_cluster(vdisks, &node, &cluster, &actions).await;

        let result = quorum.put(0, BobData::new(vec![], BobMeta::new(11))).await;

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
        let (quorum, backend) = create_cluster(vdisks, &node, &cluster, &actions).await;

        let result = quorum.put(0, BobData::new(vec![], BobMeta::new(11))).await;

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
        let (quorum, _) = create_cluster(vdisks, &node, &cluster, &actions).await;

        let result = quorum.get(101).await;

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
        let (quorum, _) = create_cluster(vdisks, &node, &cluster, &actions).await;

        let result = quorum.get(102).await;

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
        let (quorum, _) = create_cluster(vdisks, &node, &cluster, &actions).await;

        let result = quorum.get(110).await;

        assert!(result.is_ok());
        assert_eq!(1, result.unwrap().meta().timestamp());
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
        let (quorum, _) = create_cluster(vdisks, &node, &cluster, &actions).await;

        let result = quorum.get(110).await;

        assert!(result.is_ok());
        assert_eq!(1, result.unwrap().meta().timestamp());
        assert_eq!(1, calls[0].1.get_count());
    }
}
