use bob::{
    build_info::BuildInfo, init_counters, BobApiServer, BobServer, ClusterConfig, NodeConfig, Factory, Grinder,
    VirtualMapper, BackendType,
};
use clap::{crate_version, App, Arg, ArgMatches};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::runtime::Handle;
use tonic::transport::Server;
use std::path::PathBuf;
use std::fs::create_dir;

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

    check_folders(&node, matches.is_present("init_folders"));

    let mut mapper = VirtualMapper::new(&node, &cluster).await;

    let bind = node.bind();
    let bind_read = bind.lock().expect("mutex");
    let mut addr = match (
        node.bind_to_ip_address(),
        bind_read.parse(),
        port_from_address(bind_read.as_str()),
    ) {
        (Some(addr1), Ok(addr2), _) => {
            if addr1 == addr2 {
                Some(addr1)
            } else {
                error!("Addresses provided in node config and cluster config are not equal: {:?} != {:?}", addr1, addr2);
                None
            }
        }
        (Some(addr), _, _) => Some(addr),
        (_, Ok(addr), _) => Some(addr),
        (_, _, Some(port)) => Some(bind_all_interfaces(port)),
        _ => None,
    }
    .expect("Can't determine ip address to bind");

    let node_name = matches.value_of("name");
    if let Some(name) = node_name {
        let found = cluster
            .nodes()
            .iter()
            .find(|n| n.name() == name)
            .unwrap_or_else(|| panic!("cannot find node: '{}' in cluster config", name));
        mapper = VirtualMapper::new(&node, &cluster).await;
        addr = if let Ok(addr) = found.address().parse() {
            addr
        } else if let Some(port) = port_from_address(found.address()) {
            bind_all_interfaces(port)
        } else {
            panic!("Can't determine ip address to bind");
        };
    }
    warn!("Start listening on: {:?}", addr);

    let (metrics, shared_metrics) = init_counters(&node, &addr.to_string()).await;

    let handle = Handle::current();
    let bob = BobServer::new(Grinder::new(mapper, &node).await, handle, shared_metrics);

    info!("Start backend");
    bob.run_backend().await.unwrap();
    info!("Start API server");
    let http_api_port = matches
        .value_of("http_api_port")
        .and_then(|v| v.parse().ok())
        .unwrap_or(node.http_api_port());
    let http_api_address = matches
        .value_of("http_api_address")
        .and_then(|v| v.parse().ok())
        .unwrap_or(node.http_api_address());
    bob.run_api_server(http_api_address, http_api_port);

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

fn bind_all_interfaces(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port)
}

fn port_from_address(addr: &str) -> Option<u16> {
    addr.rsplit_once(':')
        .and_then(|(_, port)| port.parse::<u16>().ok())
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

fn check_folders(node: &NodeConfig, init_flag: bool) {
    if let BackendType::Pearl = node.backend_type() {
        let root_dir = node.pearl().settings().root_dir_name();
        let alien_dir = node.pearl().settings().alien_root_dir_name();

        let p_mutex = node.disks();
        let paths = p_mutex.lock().expect("node disks mutex");
        for i in 0..paths.len() {
            let mut bob_path = PathBuf::from(paths[i].path());
            bob_path.push(root_dir);
            let bob_path = bob_path.as_path();
            if !bob_path.is_dir() {
                if init_flag {
                    create_dir(bob_path).expect("Failed to create bob folder");
                } else {
                    let bob_path_str = bob_path.to_str().unwrap();
                    error!("{} folder doesn't exist, try to use --init_folders flag", bob_path_str);
                    panic!("{} folder doesn't exist, try to use --init_folders flag", bob_path_str);
                }
            }

            let mut alien_path = PathBuf::from(paths[i].path());
            alien_path.push(alien_dir);
            let alien_path = alien_path.as_path();
            if !alien_path.is_dir() {
                if init_flag {
                    create_dir(alien_path).expect("Failed to create alien folder");
                } else {
                    let alien_path_str = alien_path.to_str().unwrap();
                    error!("{} folder doesn't exist, try to use --init_folders flag", alien_path_str);
                    panic!("{} folder doesn't exist, try to use --init_folders flag", alien_path_str);
                }
            }
        }
    }
}

fn get_matches<'a>() -> ArgMatches<'a> {
    let ver = format!("{}\n{}", crate_version!(), BuildInfo::new());
    App::new("bobd")
        .version(ver.as_str())
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
            Arg::with_name("http_api_address")
                .help("http api address")
                .short("h")
                .long("host")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("http_api_port")
                .help("http api port")
                .short("p")
                .long("port")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("init_folders")
                .help("Initializes bob and alien folders")
                .long("init_folders")
                .takes_value(false),
        )
        .get_matches()
}
