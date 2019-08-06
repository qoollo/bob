#![feature(async_await)]
use bob::api::grpc::server;

use bob::core::bob_client::BobClientFactory;
use bob::core::data::VDiskMapper;
use bob::core::grinder::Grinder;
use clap::{App, Arg};
use env_logger;
use tokio::net::TcpListener;
use tokio::runtime::Builder;

use bob::core::configs::cluster::ClusterConfigYaml;
use bob::core::configs::node::{DiskPath, NodeConfigYaml};

use bob::core::server::BobSrv;

use futures::{Future, Stream};
use tower_hyper::server::{Http, Server};

use futures03::executor::ThreadPoolBuilder;
use futures03::future::{FutureExt, TryFutureExt};

use std::net::SocketAddr;

#[macro_use]
extern crate log;
extern crate dipstick;

use bob::core::metrics;

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
    let (vdisks, cluster) = ClusterConfigYaml {}.get(cluster_config).unwrap();

    let node_config = matches.value_of("node").expect("expect node config");
    println!("Node config: {:?}", node_config);
    let node = NodeConfigYaml {}.get(node_config, &cluster).unwrap();

    env_logger::builder()
        .filter_module("bob", node.log_level())
        .init();

    let mut mapper = VDiskMapper::new(vdisks.to_vec(), &node);
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
        mapper = VDiskMapper::new2(vdisks.to_vec(), name, &disks);
        addr = finded.address().parse().unwrap();
    }

    let metrics = metrics::init_counters(&node, addr.to_string());

    let backend_pool = ThreadPoolBuilder::new().pool_size(2).create().unwrap(); //TODO

    let bob = BobSrv {
        grinder: std::sync::Arc::new(Grinder::new(mapper, &node, backend_pool.clone())),
    };

    let pool = ThreadPoolBuilder::new()
        .pool_size(node.ping_threads_count() as usize)
        .create()
        .unwrap();

    let mut rt = Builder::new()
        .core_threads(
            matches
                .value_of("threads")
                .unwrap_or_default()
                .parse()
                .unwrap(),
        )
        .build()
        .unwrap();

    let executor = rt.executor();

    let b1 = bob.clone();
    let q1 = async move {
        b1.run_backend()
            .await
            .map(|_r| {})
            .map_err(|e| panic!("init failed: {:?}", e))
    };
    rt.block_on(q1.boxed().compat()).unwrap();
    info!("Start backend");

    let factory =
        BobClientFactory::new(executor, node.timeout(), node.grpc_buffer_bound(), metrics);
    let b = bob.clone();
    let q = async move { b.get_periodic_tasks(factory, pool).await };
    rt.spawn(q.boxed().compat());

    let new_service = server::BobApiServer::new(bob);

    let mut server = Server::new(new_service);

    info!("Listen on {:?}", addr);
    let bind = TcpListener::bind(&addr).expect("bind");
    let http = Http::new().http2_only(true).clone();

    let serve = bind
        .incoming()
        .for_each(move |sock| {
            if let Err(e) = sock.set_nodelay(true) {
                return Err(e);
            }

            let serve = server.serve_with(sock, http.clone());
            tokio::spawn(serve.map_err(|e| error!("Server h2 error: {:?}", e)));

            Ok(())
        })
        .map_err(|e| error!("accept error: {}", e));

    rt.spawn(serve);
    rt.shutdown_on_idle().wait().unwrap();
}
