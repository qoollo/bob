use bob::client::Factory;
use bob::configs::cluster::ConfigYaml as ClusterConfigYaml;
use bob::configs::node::{DiskPath, NodeConfigYaml};
use bob::grinder::Grinder;
use bob::grpc::bob_api_server::BobApiServer;
use bob::mapper::Virtual;
use bob::metrics;
use bob::server::BobSrv;
use clap::{App, Arg};
use futures::future::FutureExt;
use std::net::SocketAddr;
use tonic::transport::Server;

#[macro_use]
extern crate log;

#[tokio::main]
async fn main() {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
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
        .arg(
            Arg::with_name("http_api_port")
                .help("http api port")
                .default_value("8000")
                .short("p")
                .long("port")
                .takes_value(true),
        )
        .get_matches();

    if matches.value_of("cluster").is_none() {
        eprintln!("Expect cluster config");
        eprintln!("use -h for help");
        return;
    }

    if matches.value_of("node").is_none() {
        eprintln!("Expect node config");
        eprintln!("use -h for help");
        return;
    }

    let cluster_config = matches.value_of("cluster").unwrap();
    println!("Cluster config: {:?}", cluster_config);
    let (vdisks, cluster) = ClusterConfigYaml::get(cluster_config).unwrap();

    let node_config = matches.value_of("node").unwrap();
    println!("Node config: {:?}", node_config);
    let node = NodeConfigYaml::get(node_config, &cluster).unwrap();

    log4rs::init_file(node.log_config(), Default::default()).unwrap();

    let mut mapper = Virtual::new(vdisks.to_vec(), &node, &cluster);
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
                name: d.name().to_owned(),
                path: d.path().to_owned(),
            })
            .collect();
        mapper = Virtual::new(vdisks.to_vec(), &node, &cluster);
        addr = finded.address().parse().unwrap();
    }

    let metrics = metrics::init_counters(&node, &addr.to_string());

    let bob = BobSrv::new(Grinder::new(mapper, &node));

    info!("Start backend");
    bob.run_backend().await.unwrap();
    info!("Start API server");
    let http_api_port = matches
        .value_of("http_api_port")
        .and_then(|v| v.parse().ok())
        .expect("expect http_api_port port");
    bob.run_api_server(http_api_port);

    let factory = Factory::new(node.operation_timeout(), metrics);
    bob.run_periodic_tasks(factory);
    let new_service = BobApiServer::new(bob);

    Server::builder()
        .tcp_nodelay(true)
        .add_service(new_service)
        .serve(addr)
        .await
        .unwrap();
}
