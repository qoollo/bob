use bob::api::grpc::server;

use bob::core::data::{VDiskMapper, VDisk};
use bob::core::grinder::Grinder;
use clap::{App, Arg};
use env_logger;
use futures::{Future, Stream};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tower_h2::Server;

use bob::core::configs::cluster::{BobClusterConfig, ClusterConfigYaml, Cluster};
use bob::core::configs::node::{BobNodeConfig, NodeConfigYaml, DiskPath,NodeConfig};
use bob::core::server::BobSrv;

#[macro_use]
extern crate log;

fn build_bobs(vdisks: &[VDisk], cluster: &Cluster, node_config: &NodeConfig)-> Vec<(BobSrv, String)>{
    let mut bobs = Vec::new();
    for node in cluster.nodes.iter() {
        let disks: Vec<DiskPath> = node.disks
            .iter()
            .map(|d|DiskPath{name: d.name(), path: d.path()})
            .collect();
        let mapper = VDiskMapper::new2(vdisks.to_vec(), &node.name(), &disks);
        let bob = BobSrv {
            grinder: std::sync::Arc::new(Grinder::new(mapper, node_config)),
        };
        bobs.push((bob, node.address()));
    }
    bobs
}

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
        .get_matches();

    let mut rt = Runtime::new().unwrap();

    let cluster_config = matches.value_of("cluster").expect("expect cluster config");
    println!("Cluster config: {:?}", cluster_config);
    let (disks, cluster) = ClusterConfigYaml {}.get(cluster_config).unwrap();

    let node_config = matches.value_of("node").expect("expect node config");
    println!("Node config: {:?}", node_config);
    let node = NodeConfigYaml {}.get(node_config, &cluster).unwrap();

    env_logger::builder()
        .filter_module("bob", node.log_level())
        .init();

    let bobs = build_bobs(&disks, &cluster, &node);
    for (b, address) in bobs.iter() {
        rt.spawn(b.get_periodic_tasks(rt.executor()));
        let new_service = server::BobApiServer::new(b.clone());
        let h2_settings = Default::default();
        let mut h2 = Server::new(new_service, h2_settings, rt.executor());

        let addr = address.parse().unwrap();
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
    }
    rt.shutdown_on_idle().wait().unwrap();
}
