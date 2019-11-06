use bob::client::BobClientFactory;
use bob::configs::cluster::ClusterConfigYaml;
use bob::configs::node::{DiskPath, NodeConfigYaml};
use bob::grinder::Grinder;
use bob::grpc::server::BobApiServer;
use bob::mapper::VDiskMapper;
use bob::metrics;
use bob::server::BobSrv;
use bob::service::ServerSvc;
use clap::{App, Arg};
use futures::executor::ThreadPoolBuilder;
use futures::future::FutureExt;
use hyper::server::conn::AddrIncoming;
use hyper::Server;
use std::net::SocketAddr;

#[macro_use]
extern crate log;

#[tokio::main]
async fn main() {
    let matches = App::new("Bob")
        .arg(
            Arg::with_name("cluster")
                .help("cluster config file")
                .takes_value(true)
                .short("c")
                .long("cluster"),
        )
        .arg(
            Arg::with_name("node")
                .help("node config file")
                .takes_value(true)
                .short("n")
                .long("node"),
        )
        .arg(
            Arg::with_name("name")
                .help("node name")
                .takes_value(true)
                .short("a")
                .long("name"),
        )
        .arg(
            Arg::with_name("threads")
                .help("count threads")
                .takes_value(true)
                .short("t")
                .long("threads")
                .default_value("4"),
        )
        .get_matches();

    let cluster_config = matches.value_of("cluster").expect("expect cluster config");
    println!("Cluster config: {:?}", cluster_config);
    let (vdisks, cluster) = ClusterConfigYaml::get(cluster_config).unwrap();

    let node_config = matches.value_of("node").expect("expect node config");
    println!("Node config: {:?}", node_config);
    let node = NodeConfigYaml::get(node_config, &cluster).unwrap();

    log4rs::init_file(node.log_config(), Default::default()).unwrap();

    let mut mapper = VDiskMapper::new(vdisks.to_vec(), &node, &cluster);
    let mut addr: SocketAddr = node.bind().parse().unwrap();

    let node_name = matches.value_of("name");
    if node_name.is_some() {
        let name = node_name.unwrap();
        let finded = cluster
            .nodes
            .iter()
            .find(|n| n.name() == name)
            .unwrap_or_else(|| panic!("cannot find node: '{}' in cluster config", name));
        let disks: Vec<DiskPath> = finded
            .disks
            .iter()
            .map(|d| DiskPath {
                name: d.name(),
                path: d.path(),
            })
            .collect();
        mapper = VDiskMapper::new_direct(vdisks.to_vec(), name, &disks, &cluster);
        addr = finded.address().parse().unwrap();
    }

    let metrics = metrics::init_counters(&node, addr.to_string());

    let backend_pool = ThreadPoolBuilder::new().pool_size(2).create().unwrap(); //TODO

    let bob = BobSrv {
        grinder: std::sync::Arc::new(Grinder::new(mapper, &node, backend_pool.clone())),
    };

    let executor = rt.executor();

    info!("Start backend");
    bob.run_backend().await.unwrap();
    info!("Start API server");
    bob.run_api_server();

    let factory =
        BobClientFactory::new(executor, node.timeout(), node.grpc_buffer_bound(), metrics);
    let b = bob.clone();
    tokio::spawn(async move { b.get_periodic_tasks(factory).map(|r| r.unwrap()).await });
    let new_service = BobApiServer::new(bob);
    let svc = ServerSvc(new_service);

    Server::builder(AddrIncoming::bind(&addr).unwrap())
        .tcp_nodelay(true)
        .serve(svc)
        .await
        .unwrap();
}
