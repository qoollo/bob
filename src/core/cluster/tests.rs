use super::prelude::*;
use crate::core::configs::{cluster::tests::cluster_config, node::tests::node_config};
use std::sync::atomic::{AtomicU64, Ordering};

#[allow(dead_code)]
fn init_logger() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .try_init()
        .unwrap();
}

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
    info!("get ok timestamp");
    client.expect_get().returning(move |_key, _options| {
        call.get_inc();
        test_utils::get_ok(node.name().to_owned(), timestamp)
    });
}

fn get_err(client: &mut BobClient, node: Node, call: Arc<CountCall>) {
    info!("get err");
    client.expect_get().returning(move |_key, _options| {
        info!("mock client returning closure");
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
        debug!("increment get count");
        self.get_count.fetch_add(1, Ordering::SeqCst);
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
    info!("create ok node: {}, op: {:?}", name, op);
    create_node(name, (op.0, op.1, 0))
}

fn create_node(name: &str, op: (bool, bool, u64)) -> (&str, Call, Arc<CountCall>) {
    let call = move |client: &mut BobClient, n: Node, call: Arc<CountCall>| {
        let f = |client: &mut BobClient, n: Node, c: Arc<CountCall>, op: (bool, bool, u64)| {
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
    };
    let call = Box::new(call);
    (name, call, Arc::new(CountCall::new()))
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
    let get = backend.get_local(key, Operation::new_alien(0)).await;
    assert!(get.err().unwrap().is_key_not_found());
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

    let get = backend.get_local(key, Operation::new_alien(0)).await;
    assert!(get.err().unwrap().is_key_not_found());
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
    let mut get = backend.get_local(key, Operation::new_alien(0)).await;
    assert!(get.err().unwrap().is_key_not_found());
    let key = 4;
    get = backend.get_local(key, Operation::new_alien(0)).await;
    assert!(get.err().unwrap().is_key_not_found());
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

    let get = backend.get_local(5, Operation::new_alien(0)).await;
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

    let get = backend.get_local(5, Operation::new_alien(0)).await;
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

    let get = backend.get_local(0, Operation::new_alien(0)).await;
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

    let get = backend.get_local(0, Operation::new_alien(0)).await;
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

    let get = backend.get_local(0, Operation::new_alien(0)).await;
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

    let get = backend.get_local(0, Operation::new_alien(0)).await;
    assert!(get.is_ok());
}

/// 1 node, 1 vdisk, 1 replics in vdisk, quorum = 1
/// get no data => err
#[tokio::test]
async fn simple_one_node_get_err() {
    // init_logger();
    info!("logger initialized");
    let (vdisks, node, cluster) = prepare_configs(1, 1, 1, 1);
    info!("configs prepared");
    let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![create_ok_node("0", (true, false))];
    info!("actions created");
    let (quorum, _) = create_cluster(vdisks, &node, &cluster, &actions).await;
    info!("cluster created");
    let result = quorum.get(102).await;
    info!("request finished");
    assert!(result.is_err());
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

    let (quorum, _) = create_cluster(vdisks, &node, &cluster, &actions).await;

    let result = quorum.get(110).await;

    assert!(result.is_ok());
    assert_eq!(1, result.unwrap().meta().timestamp());
}

// 2 nodes, 2 vdisk, 1 replics in vdisk, quorum = 1
// get data from 1 nodes => fail => read from sup node => ok
// #[tokio::test]
// async fn simple_two_node_read_sup_get_ok() {
//     // log4rs::init_file("./logger.yaml", Default::default()).unwrap();
//     let (vdisks, node, cluster) = prepare_configs(2, 2, 1, 1);

//     let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
//         create_node("0", (true, false, 0)),
//         create_node("1", (true, true, 1)),
//     ];

//     let calls: Vec<_> = actions
//         .iter()
//         .map(|(name, _, call)| ((*name).to_string(), call.clone()))
//         .collect();
//     let (quorum, _) = create_cluster(vdisks, &node, &cluster, &actions).await;

//     let result = quorum.get(110).await;
//     dbg!(&result);

//     assert!(result.is_err());
//     assert_eq!(1, result.unwrap().meta().timestamp());
//     assert_eq!(1, calls[0].1.get_count());
// }
