#![feature(async_await)]
use bob::api::grpc::server;

use bob::core::data::{VDisk, VDiskMapper};
use bob::core::grinder::Grinder;
use clap::{App, Arg};
use env_logger;
use futures::{Future, Stream};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

use bob::core::configs::cluster::{ClusterConfig, ClusterConfigYaml};
use bob::core::configs::node::{DiskPath, NodeConfig, NodeConfigYaml};
use bob::core::server::BobSrv;

use futures03::executor::ThreadPoolBuilder;
use futures03::future::{FutureExt, TryFutureExt};
use tower_hyper::server::Server;

#[macro_use]
extern crate log;

fn build_bobs(
    vdisks: &[VDisk],
    cluster: &ClusterConfig,
    node_config: &NodeConfig,
) -> Vec<(BobSrv, String)> {
    let mut bobs = Vec::new();
    for node in cluster.nodes.iter() {
        let disks: Vec<DiskPath> = node
            .disks
            .iter()
            .map(|d| DiskPath {
                name: d.name(),
                path: d.path(),
            })
            .collect();
        let mapper = VDiskMapper::new2(vdisks.to_vec(), &node.name(), &disks);
        let backend_pool = ThreadPoolBuilder::new().pool_size(2).create().unwrap(); //TODO
        let bob = BobSrv {
            grinder: std::sync::Arc::new(Grinder::new(mapper, node_config, backend_pool)),
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

    let cluster_config = matches.value_of("cluster").expect("expect cluster config");
    println!("Cluster config: {:?}", cluster_config);
    let (disks, cluster) = ClusterConfigYaml {}.get(cluster_config).unwrap();

    let node_config = matches.value_of("node").expect("expect node config");
    println!("Node config: {:?}", node_config);
    let node = NodeConfigYaml {}.get(node_config, &cluster).unwrap();

    env_logger::builder()
        .filter_module("bob", node.log_level())
        .init();

    let rt = Runtime::new().unwrap();
    let bobs = build_bobs(&disks, &cluster, &node);
    for (b, address) in bobs.into_iter() {
        let pool = ThreadPoolBuilder::new()
            .pool_size(node.ping_threads_count() as usize)
            .create()
            .unwrap();

        let mut rt = Runtime::new().unwrap();
        let executor = rt.executor();

        let bob = b.clone();
        let q = async move { bob.get_periodic_tasks(executor, pool).await };
        rt.spawn(q.boxed().compat());

        let b1 = b.clone();
        let q1 = async move {
            b1.run_backend()
                .await
                .map(|_r| {})
                .map_err(|e| panic!("init failed: {:?}", e))
        };
        rt.spawn(q1.boxed().compat());

        let new_service = server::BobApiServer::new(b.clone());
        let mut server = Server::new(new_service);

        let addr = address.parse().unwrap();
        let bind = TcpListener::bind(&addr).expect("bind");
        let serve = bind
            .incoming()
            .for_each(move |sock| {
                if let Err(e) = sock.set_nodelay(true) {
                    return Err(e);
                }

                let serve = server.serve(sock);
                tokio::spawn(serve.map_err(|e| error!("Server h2 error: {:?}", e)));

                Ok(())
            })
            .map_err(|e| error!("accept error: {}", e));

        rt.spawn(serve);
    }
    rt.shutdown_on_idle().wait().unwrap();
}
