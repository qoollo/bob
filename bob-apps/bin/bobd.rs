use bob::{init_counters, BobApiServer, BobServer, ClusterConfig, Factory, Grinder, VirtualMapper};
use clap::{App, Arg, ArgMatches};
use std::net::ToSocketAddrs;
use tokio::runtime::Handle;
use tonic::transport::Server;

#[macro_use]
extern crate log;

#[tokio::main]
async fn main() {
    let matches = get_matches();

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
    let cluster = ClusterConfig::try_get(cluster_config).await.unwrap();

    let node_config = matches.value_of("node").unwrap();
    println!("Node config: {:?}", node_config);
    let node = cluster.get(node_config).await.unwrap();

    log4rs::init_file(node.log_config(), Default::default()).expect("can't find log config");

    let mut mapper = VirtualMapper::new(&node, &cluster).await;
    let mut addr = node.bind().to_socket_addrs().unwrap().next().unwrap();

    let node_name = matches.value_of("name");
    if let Some(name) = node_name {
        let finded = cluster
            .nodes()
            .iter()
            .find(|n| n.name() == name)
            .unwrap_or_else(|| panic!("cannot find node: '{}' in cluster config", name));
        mapper = VirtualMapper::new(&node, &cluster).await;
        addr = finded.address().to_socket_addrs().unwrap().next().unwrap();
    }

    let (metrics, shared_metrics) = init_counters(&node, &addr.to_string());

    let handle = Handle::current();
    let bob = BobServer::new(Grinder::new(mapper, &node).await, handle, shared_metrics);

    info!("Start backend");
    bob.run_backend().await.unwrap();
    info!("Start API server");
    let http_api_port = matches
        .value_of("http_api_port")
        .and_then(|v| v.parse().ok())
        .expect("expect http_api_port port");
    bob.run_api_server(http_api_port);

    create_signal_handlers(&bob).unwrap();

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

fn create_signal_handlers(server: &BobServer) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::signal::unix::SignalKind;
    let signals = [SignalKind::terminate(), SignalKind::interrupt()];
    for s in signals.iter() {
        spawn_signal_handler(server, *s)?;
    }
    Ok(())
}

fn spawn_signal_handler(
    server: &BobServer,
    s: tokio::signal::unix::SignalKind,
) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::signal::unix::signal;
    let mut task = signal(s)?;
    let server = server.clone();
    tokio::spawn(async move {
        task.recv().await;
        debug!("Got signal {:?}", s);
        server.shutdown().await;
        std::process::exit(0);
    });
    Ok(())
}

fn get_matches<'a>() -> ArgMatches<'a> {
    App::new(env!("CARGO_PKG_NAME"))
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
        .get_matches()
}
