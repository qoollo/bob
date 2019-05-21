use bob::api::grpc::server;

use bob::core::data::VDiskMapper;
use bob::core::grinder::Grinder;
use clap::{App, Arg};
use env_logger;
use futures::{Future, Stream};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tower_h2::Server;

use bob::core::configs::cluster::{BobClusterConfig, ClusterConfigYaml};
use bob::core::configs::node::{BobNodeConfig, DiskPath, NodeConfigYaml};

use bob::core::server::BobSrv;

#[macro_use]
extern crate log;

fn main() {
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
        .get_matches();

    let mut rt = Runtime::new().unwrap();

    let cluster_config = matches.value_of("cluster").expect("expect cluster config");
    println!("Cluster config: {:?}", cluster_config);
    let (vdisks, cluster) = ClusterConfigYaml {}.get(cluster_config).unwrap();

    let node_config = matches.value_of("node").expect("expect node config");
    println!("Node config: {:?}", node_config);
    let node = NodeConfigYaml {}.get(node_config, &cluster).unwrap();

    env_logger::builder()
        .filter_module("bob", node.log_level())
        .init();

    let mut mapper = VDiskMapper::new(vdisks.to_vec(), &node);
    let mut addr = node.bind().parse().unwrap();

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
        mapper = VDiskMapper::new2(vdisks.to_vec(), name, &disks);
        addr = finded.address().parse().unwrap();
    }
    let bob = BobSrv {
        grinder: std::sync::Arc::new(Grinder::new(mapper, &node)),
    };

    rt.spawn(bob.get_periodic_tasks(rt.executor()));
    let new_service = server::BobApiServer::new(bob);

    let h2_settings = Default::default();
    let mut h2 = Server::new(new_service, h2_settings, rt.executor());

    info!("Listen on {:?}", addr);
    let bind = TcpListener::bind(&addr).expect("bind");

    let serve = bind
        .incoming()
        .for_each(move |sock| {
            if let Err(e) = sock.set_nodelay(true) {
                return Err(e);
            }

            let serve = h2.serve(sock);
            tokio::spawn(serve.map_err(|e| error!("Server h2 error: {:?}", e)));

            Ok(())
        })
        .map_err(|e| error!("accept error: {}", e));

    rt.spawn(serve);
    rt.shutdown_on_idle().wait().unwrap();
}
