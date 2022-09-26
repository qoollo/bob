use std::sync::atomic::{AtomicU64, Ordering};

use crate::{
    prelude::*,
    {cluster::Cluster, test_utils},
};

use bob_common::{
    bob_client::b_client::MockBobClient as BobClient,
    configs::{
        cluster::{tests::cluster_config, Cluster as ClusterConfig},
        node::tests::node_config,
    },
    data::BobMeta,
};
use tokio::time::sleep;

use super::quorum::Quorum;

fn ping_ok(client: &mut BobClient, node: Node) {
    let cl = node;

    client
        .expect_ping()
        .returning(move || test_utils::ping_ok(cl.name().to_owned()));
}

fn put_ok(client: &mut BobClient, node: Node, call: Arc<CountCall>) {
    client.expect_put().returning(move |_key, _data, _options| {
        call.put_inc();
        warn!("OK OK");
        test_utils::put_ok(node.name().to_owned())
    });
}

fn put_err(client: &mut BobClient, node: Node, call: Arc<CountCall>) {
    debug!("mock BobClient return error on PUT");
    client.expect_put().returning(move |_key, _data, _options| {
        call.put_inc();
        test_utils::put_err(node.name().to_owned())
    });
}

fn get_ok_timestamp(client: &mut BobClient, node: Node, call: Arc<CountCall>, timestamp: u64) {
    trace!("get ok timestamp");
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

#[allow(dead_code)]
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

    #[allow(dead_code)]
    fn put_inc(&self) {
        self.put_count.fetch_add(1, Ordering::SeqCst);
    }

    fn put_count(&self) -> u64 {
        self.put_count.load(Ordering::Relaxed)
    }

    fn get_count(&self) -> u64 {
        self.get_count.load(Ordering::Relaxed)
    }

    #[allow(dead_code)]
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
) -> (NodeConfig, ClusterConfig) {
    let node = node_config("0", quorum);
    let cluster = cluster_config(count_nodes, count_vdisks, count_replicas);
    cluster.check(&node).expect("check node config");
    (node, cluster)
}

async fn create_cluster(
    node: &NodeConfig,
    cluster: &ClusterConfig,
    map: &[(&str, Call, Arc<CountCall>)],
) -> (Quorum, Arc<Backend>) {
    let mapper = Arc::new(Virtual::new(node, cluster).await);
    for node in mapper.nodes().values() {
        let mut mock_client = BobClient::new();

        let (_, func, call) = map
            .iter()
            .find(|(name, _, _)| *name == node.name())
            .expect("find node with name");
        func(&mut mock_client, node.clone(), call.clone());
        node.set_connection(mock_client);
    }

    let backend = Arc::new(Backend::new(mapper.clone(), node).await);
    (Quorum::new(backend.clone(), mapper, node.quorum()), backend)
}

fn create_ok_node(name: &str, set_put_ok: bool, set_get_ok: bool) -> (&str, Call, Arc<CountCall>) {
    info!(
        "create ok node: {}, set_put_ok: {}, set_get_ok: {}",
        name, set_put_ok, set_get_ok
    );
    create_node(name, set_put_ok, set_get_ok, 0)
}

fn create_node(
    name: &str,
    set_put_ok: bool,
    set_get_ok: bool,
    returned_timestamp: u64,
) -> (&str, Call, Arc<CountCall>) {
    let call = move |client: &mut BobClient, n: Node, call: Arc<CountCall>| {
        let f = |client: &mut BobClient,
                 n: Node,
                 c: Arc<CountCall>,
                 set_put_ok: bool,
                 set_get_ok: bool,
                 timestamp: u64| {
            ping_ok(client, n.clone());
            if set_put_ok {
                put_ok(client, n.clone(), c.clone());
            } else {
                debug!("node fn set to put_err");
                put_err(client, n.clone(), c.clone());
            }
            if set_get_ok {
                get_ok_timestamp(client, n, c, timestamp);
            } else {
                get_err(client, n, c);
            }
        };
        f(
            client,
            n.clone(),
            call.clone(),
            set_put_ok,
            set_get_ok,
            returned_timestamp,
        );
        client.expect_clone().returning(move || {
            let mut cl = BobClient::default();
            f(
                &mut cl,
                n.clone(),
                call.clone(),
                set_put_ok,
                set_get_ok,
                returned_timestamp,
            );
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
    test_utils::init_logger();
    let (node, cluster) = prepare_configs(1, 1, 1, 1);

    let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![create_ok_node("0", true, true)];

    let (quorum, backend) = create_cluster(&node, &cluster, &actions).await;

    let key = 1;
    let result = quorum
        .put(BobKey::from(key), BobData::new(vec![], BobMeta::new(11)))
        .await;

    assert!(result.is_ok());
    //assert_eq!(1, calls[0].1.put_count());
    warn!("can't track put result, because it doesn't pass through mock client");
    let get = backend
        .get_local(BobKey::from(key), Operation::new_alien(0))
        .await;
    assert!(get.err().unwrap().is_key_not_found());
}

/// 2 node, 1 vdisk, 1 replics in vdisk, quorum = 1
/// put data on both nodes
/// no data local
#[tokio::test]
async fn simple_two_node_one_vdisk_cluster_put_ok() {
    test_utils::init_logger();
    let (node, cluster) = prepare_configs(2, 1, 2, 1);

    let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
        create_ok_node("0", true, true),
        create_ok_node("1", true, true),
    ];

    let calls: Vec<_> = actions
        .iter()
        .map(|(name, _, call)| ((*name).to_string(), call.clone()))
        .collect();
    let (quorum, backend) = create_cluster(&node, &cluster, &actions).await;
    let key = 2;
    let result = quorum
        .put(BobKey::from(key), BobData::new(vec![], BobMeta::new(11)))
        .await;
    sleep(Duration::from_millis(1)).await;

    assert!(result.is_ok());
    //assert_eq!(1, calls[0].1.put_count());
    warn!("can't track put result, because it doesn't pass through mock client");
    assert_eq!(1, calls[1].1.put_count());

    let get = backend
        .get_local(BobKey::from(key), Operation::new_alien(0))
        .await;
    assert!(get.err().unwrap().is_key_not_found());
}

/// 2 node, 2 vdisk, 1 replics in vdisk, quorum = 1
/// put first data to "1" node, check no data on "2" node
/// put second data to "2" node, check one on "2" node
/// no data local
#[tokio::test]
async fn simple_two_node_two_vdisk_one_replica_cluster_put_ok() {
    test_utils::init_logger();
    let (node, cluster) = prepare_configs(2, 2, 1, 1);

    let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
        create_ok_node("0", true, true),
        create_ok_node("1", true, true),
    ];

    let calls: Vec<_> = actions
        .iter()
        .map(|(name, _, call)| ((*name).to_string(), call.clone()))
        .collect();
    let (quorum, backend) = create_cluster(&node, &cluster, &actions).await;

    let mut result = quorum
        .put(BobKey::from(3), BobData::new(vec![], BobMeta::new(11)))
        .await;

    assert!(result.is_ok());
    //assert_eq!(0, calls[0].1.put_count());
    warn!("can't track put result, because it doesn't pass through mock client");
    assert_eq!(1, calls[1].1.put_count());

    result = quorum
        .put(BobKey::from(4), BobData::new(vec![], BobMeta::new(11)))
        .await;

    assert!(result.is_ok());
    //assert_eq!(1, calls[0].1.put_count());
    warn!("can't track put result, because it doesn't pass through mock client");
    assert_eq!(1, calls[1].1.put_count());
    let key = 3;
    let mut get = backend
        .get_local(BobKey::from(key), Operation::new_alien(0))
        .await;
    assert!(get.err().unwrap().is_key_not_found());
    let key = 4;
    get = backend
        .get_local(BobKey::from(key), Operation::new_alien(0))
        .await;
    assert!(get.err().unwrap().is_key_not_found());
}

/// 2 node, 1 vdisk, 2 replics in vdisk, quorum = 2
/// one node failed => write one data local => no quorum => put err
#[tokio::test]
async fn two_node_one_vdisk_cluster_one_node_failed_put_err() {
    test_utils::init_logger();
    let (node, cluster) = prepare_configs(2, 1, 2, 2);
    // debug!("cluster: {:?}", cluster);
    let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
        create_ok_node("0", true, true),
        create_ok_node("1", false, true),
    ];

    let calls: Vec<_> = actions
        .iter()
        .map(|(name, _, call)| ((*name).to_string(), call.clone()))
        .collect();
    let (quorum, backend) = create_cluster(&node, &cluster, &actions).await;

    let result = quorum
        .put(BobKey::from(5), BobData::new(vec![], BobMeta::new(11)))
        .await;
    sleep(Duration::from_millis(1)).await;

    assert!(result.is_ok());
    // assert_eq!(1, calls[0].1.put_count());
    warn!("can't track put result, because it doesn't pass through mock client");
    assert_eq!(1, calls[1].1.put_count());

    let get = backend
        .get_local(BobKey::from(5), Operation::new_alien(0))
        .await;
    assert!(get.is_ok());
}

/// 2 nodes, 1 vdisk, 2 replicas in vdisk, quorum = 1
/// one node failed => write one data local => quorum => put ok
#[tokio::test]
async fn two_node_one_vdisk_cluster_one_node_failed_put_ok() {
    test_utils::init_logger();
    let (node, cluster) = prepare_configs(2, 1, 2, 1);
    // debug!("cluster: {:?}", cluster);
    let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
        create_ok_node("0", true, true),
        create_ok_node("1", false, true),
    ];

    let calls: Vec<_> = actions
        .iter()
        .map(|(name, _, call)| ((*name).to_string(), call.clone()))
        .collect();
    let (quorum, _) = create_cluster(&node, &cluster, &actions).await;

    let result = quorum
        .put(BobKey::from(5), BobData::new(vec![], BobMeta::new(11)))
        .await;
    sleep(Duration::from_millis(1000)).await;

    assert!(result.is_ok());
    //assert_eq!(1, calls[0].1.put_count());
    warn!("can't track put result, because it doesn't pass through mock client");
    assert_eq!(1, calls[1].1.put_count());
}

/// 3 node, 2 vdisk, 2 replics in vdisk, quorum = 2
/// one node failed => write one data local + one sup node => quorum => put ok
#[tokio::test]
async fn three_node_two_vdisk_cluster_second_node_failed_put_ok() {
    test_utils::init_logger();
    let (node, cluster) = prepare_configs(3, 2, 2, 2);
    // debug!("cluster: {:?}", cluster);
    let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
        create_ok_node("0", true, true),
        create_ok_node("1", false, true),
        create_ok_node("2", true, true),
    ];

    let calls: Vec<_> = actions
        .iter()
        .map(|(name, _, call)| ((*name).to_string(), call.clone()))
        .collect();
    let (quorum, _) = create_cluster(&node, &cluster, &actions).await;

    sleep(Duration::from_millis(1)).await;
    let result = quorum
        .put(BobKey::from(0), BobData::new(vec![], BobMeta::new(11)))
        .await;
    sleep(Duration::from_millis(1000)).await;
    assert!(result.is_ok());
    //assert_eq!(1, calls[0].1.put_count());
    warn!("can't track put result, because it doesn't pass through mock client");
    assert_eq!(1, calls[1].1.put_count());
    assert_eq!(1, calls[2].1.put_count());
}

/// 3 node, 2 vdisk, 2 replics in vdisk, quorum = 2
/// one node failed => write one data local + one sup node(failed) => quorum => put err
#[tokio::test]
async fn three_node_two_vdisk_cluster_one_node_failed_put_err() {
    test_utils::init_logger();
    let (node, cluster) = prepare_configs(3, 2, 2, 2);
    // debug!("cluster: {:?}", cluster);
    let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
        create_ok_node("0", true, true),
        create_ok_node("1", false, true),
        create_ok_node("2", false, true),
    ];

    let calls: Vec<_> = actions
        .iter()
        .map(|(name, _, call)| ((*name).to_string(), call.clone()))
        .collect();
    let (quorum, backend) = create_cluster(&node, &cluster, &actions).await;

    info!("quorum put: 0");
    let result = quorum
        .put(BobKey::from(0), BobData::new(vec![], BobMeta::new(11)))
        .await;
    sleep(Duration::from_millis(1000)).await;

    assert!(result.is_ok());
    // assert_eq!(1, calls[0].1.put_count());
    warn!("can't track put result, because it doesn't pass through mock client");
    assert_eq!(1, calls[1].1.put_count());
    assert_eq!(1, calls[2].1.put_count());

    let get = backend
        .get_local(BobKey::from(0), Operation::new_alien(0))
        .await;
    assert!(get.is_ok());
}

/// 3 node, 2 vdisk, 2 replics in vdisk, quorum = 2
/// one node failed, but call other => quorum => put ok
#[tokio::test]
async fn three_node_two_vdisk_cluster_one_node_failed_put_ok2() {
    test_utils::init_logger();
    let (node, cluster) = prepare_configs(3, 2, 2, 2);
    // debug!("cluster: {:?}", cluster);
    let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
        create_ok_node("0", true, true),
        create_ok_node("1", true, true),
        create_ok_node("2", false, true),
    ];

    let calls: Vec<_> = actions
        .iter()
        .map(|(name, _, call)| ((*name).to_string(), call.clone()))
        .collect();
    let (quorum, backend) = create_cluster(&node, &cluster, &actions).await;

    let result = quorum
        .put(BobKey::from(0), BobData::new(vec![], BobMeta::new(11)))
        .await;
    sleep(Duration::from_millis(1000)).await;

    assert!(result.is_ok());
    //assert_eq!(1, calls[0].1.put_count());
    warn!("can't track put result, because it doesn't pass through mock client");
    assert_eq!(1, calls[1].1.put_count());
    assert_eq!(0, calls[2].1.put_count());

    let get = backend
        .get_local(BobKey::from(0), Operation::new_alien(0))
        .await;
    assert!(get.is_err());
}

/// 3 node, 1 vdisk, 3 replics in vdisk, quorum = 2
/// one node failed => local write => quorum => put ok
#[tokio::test]
async fn three_node_one_vdisk_cluster_one_node_failed_put_ok() {
    test_utils::init_logger();
    let (node, cluster) = prepare_configs(3, 1, 3, 2);
    // debug!("cluster: {:?}", cluster);
    let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
        create_ok_node("0", true, true),
        create_ok_node("1", true, true),
        create_ok_node("2", false, true),
    ];

    let calls: Vec<_> = actions
        .iter()
        .map(|(name, _, call)| ((*name).to_string(), call.clone()))
        .collect();
    let (quorum, backend) = create_cluster(&node, &cluster, &actions).await;

    info!("put local: 0");
    let result = quorum
        .put(BobKey::from(0), BobData::new(vec![], BobMeta::new(11)))
        .await;
    assert!(result.is_ok());
    // assert_eq!(1, calls[0].1.put_count());
    warn!("can't track put result, because it doesn't pass through mock client");
    assert_eq!(1, calls[1].1.put_count());
    assert_eq!(1, calls[2].1.put_count());

    sleep(Duration::from_millis(32)).await;
    info!("get local backend: 0");
    let get = backend
        .get_local(BobKey::from(0), Operation::new_alien(0))
        .await;
    debug!("{:?}", get);
    assert!(get.is_ok());
}

/// 1 node, 1 vdisk, 1 replics in vdisk, quorum = 1
/// get no data => err
#[tokio::test]
async fn simple_one_node_get_err() {
    // test_utils::init_logger();
    info!("logger initialized");
    let (node, cluster) = prepare_configs(1, 1, 1, 1);
    info!("configs prepared");
    let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![create_ok_node("0", true, false)];
    info!("actions created");
    let (quorum, _) = create_cluster(&node, &cluster, &actions).await;
    info!("cluster created");
    let result = quorum.get(BobKey::from(102)).await;
    info!("request finished");
    assert!(result.is_err());
}

/// 2 nodes, 1 vdisk, 2 replics in vdisk, quorum = 2
/// get data from 2 nodes => get differ timetsmps => get max => ok
#[tokio::test]
async fn simple_two_node_get_ok() {
    // log4rs::init_file("./logger.yaml", Default::default()).unwrap();
    let (node, cluster) = prepare_configs(2, 1, 2, 2);

    let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
        create_node("0", true, true, 0),
        create_node("1", true, true, 1),
    ];

    let (quorum, _) = create_cluster(&node, &cluster, &actions).await;

    let result = quorum.get(BobKey::from(110)).await;

    assert!(result.is_ok());
    assert_eq!(1, result.unwrap().meta().timestamp());
}

// 2 nodes, 2 vdisk, 1 replics in vdisk, quorum = 1
// get data from 1 nodes => fail => read from sup node => ok
#[tokio::test]
async fn simple_two_node_read_sup_get_ok() {
    // log4rs::init_file("./logger.yaml", Default::default()).unwrap();
    let (node, cluster) = prepare_configs(2, 2, 1, 1);
    let actions: Vec<(&str, Call, Arc<CountCall>)> = vec![
        create_node("0", true, false, 0),
        create_node("1", true, true, 1),
    ];

    let calls: Vec<_> = actions
        .iter()
        .map(|(name, _, call)| ((*name).to_string(), call.clone()))
        .collect();
    let (quorum, _) = create_cluster(&node, &cluster, &actions).await;

    let result = quorum.get(BobKey::from(110)).await;
    dbg!(&result);

    assert!(result.is_ok());
    assert_eq!(1, result.unwrap().meta().timestamp());
    assert_eq!(1, calls[1].1.get_count());
}
