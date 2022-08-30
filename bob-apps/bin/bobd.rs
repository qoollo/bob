use bob::{
    build_info::BuildInfo, init_counters, BobApiServer, BobServer, ClusterConfig, NodeConfig, Factory, Grinder,
    VirtualMapper, BackendType,
};
use bob_access::{Authenticator, BasicAuthenticator, Credentials, StubAuthenticator, UsersMap, AuthenticationType};
use clap::{crate_version, App, Arg, ArgMatches, SubCommand};
use std::{
    collections::HashMap,
    error::Error as ErrorTrait,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
use tokio::{net::lookup_host, runtime::Handle, signal::unix::SignalKind};
use tonic::transport::Server;
use std::path::PathBuf;
use std::fs::create_dir;

#[macro_use]
extern crate log;

use log4rs::append::console::ConsoleAppender;
use log4rs::encode::pattern::PatternEncoder;
use log4rs::append::rolling_file::{
    RollingFileAppender,
    policy::compound::{
        trigger::size::SizeTrigger,
        roll::fixed_window::FixedWindowRoller,
        CompoundPolicy
    }
};
use log4rs::config::{Appender, Config, Logger, Root};

use network_interface::{NetworkInterface, NetworkInterfaceConfig, Addr};

use anyhow::{anyhow, Context, Result as AnyResult};

#[tokio::main]
async fn main() {
    let matches = get_matches();

    let cluster;
    let node;
    if let (sc, Some(sub_matches)) = matches.subcommand() {
        match sc {
            "testmode" => match configure_testmode(sub_matches) {
                Ok((c, n)) => {
                    cluster = c;
                    node = n;
                }
                Err(e) => {
                    eprintln!("Initialization error: {}", e);
                    eprintln!("use --help");
                    return;
                }
            },
            _ => unreachable!("unknown command"),
        }
    } else {
        if matches.value_of("cluster").is_none() {
            eprintln!("Expect cluster config");
            eprintln!("use --help");
            return;
        }

        if matches.value_of("node").is_none() {
            eprintln!("Expect node config");
            eprintln!("use --help");
            return;
        }

        let cluster_config = matches.value_of("cluster").unwrap();
        println!("Cluster config: {:?}", cluster_config);
        cluster = ClusterConfig::try_get(cluster_config).await.unwrap();

        let node_config_file = matches.value_of("node").unwrap();
        println!("Node config: {:?}", node_config_file);
        node = cluster.get(node_config_file).await.unwrap();

        log4rs::init_file(node.log_config(), log4rs_logstash::config::deserializers())
            .expect("can't find log config");
        check_folders(&node, matches.is_present("init_folders"));
    }

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

    info!("Start API server");
    let http_api_port = matches
        .value_of("http_api_port")
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| node.http_api_port());
    let http_api_address = matches
        .value_of("http_api_address")
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| node.http_api_address());

    let authentication_type = node.authentication_type();
    match authentication_type {
        AuthenticationType::None => {
            let authenticator = StubAuthenticator::new();
            run_server(node, authenticator, mapper, http_api_address, http_api_port, addr).await;
        }
        AuthenticationType::Basic => {
            let users_storage =
                UsersMap::from_file(node.users_config()).expect("Can't parse users and roles");
            let mut authenticator = BasicAuthenticator::new(users_storage);
            let nodes_credentials = nodes_credentials_from_cluster_config(&cluster).await;
            authenticator
                .set_nodes_credentials(nodes_credentials)
                .expect("failed to gen nodes credentials from cluster config");
            run_server(node, authenticator, mapper, http_api_address, http_api_port, addr).await;
        }
        _ => {
            warn!("valid authentication type not provided");
        }
    }
}

fn configure_testmode(sub_matches: &ArgMatches) -> AnyResult<(ClusterConfig, NodeConfig)> {
    let mut addresses = Vec::with_capacity(1);
    let port = match sub_matches.value_of("grpc-port") {
        Some(v) => v.parse().context("could not parse --grpc-port")?,
        None => 20000
    };
    let mut this_node = None;
    if let Some(node_list) = sub_matches.value_of("nodes") {
        let available_ips: Vec<String> = NetworkInterface::show()?.into_iter().filter_map(|itf|
            match itf.addr? {
                Addr::V4(addr) => {
                    Some(addr.ip.to_string())
                },
                _ => None
        }).collect();

        for (index, addr) in node_list.split(",").enumerate() {
            let split = &addr.split_once(":").context("could not find address in --nodes")?;
            let in_ip = split.0;
            let in_port = split.1.parse::<u16>().context("could not parse port in --nodes")?;
            if this_node.is_none() {
                this_node = available_ips.iter().find_map(|ip| {
                    if ip == in_ip && port == in_port {
                        Some(index)
                    } else {
                        None
                    }
                })
            }
            addresses.push(String::from(addr));
        }
        if this_node.is_none() {
            return Err(anyhow!("no available ips found"));
        }
    } else {
        this_node = Some(0);
        addresses.push(format!("127.0.0.1:{port}"))
    }
    let this_node = this_node.unwrap();
    let cluster = ClusterConfig::get_testmode(
        sub_matches.value_of("data").unwrap_or(format!("data_{this_node}").as_str()).to_string(),
        addresses)?;
    let http_api_port = match sub_matches.value_of("restapi-port") {
        Some(v) => Some(v.parse().context("could not parse --restapi-port")?),
        None => None
    };
    let node = cluster.get_testmode_node(this_node, http_api_port)?;

    init_testmode_logger(log::LevelFilter::Error);

    check_folders(&node, true);

    println!("Bob is starting");
    let n = &cluster.nodes()[this_node];
    println!("Data directory: {}", n.disks()[0].path());
    println!("gRPC API available at: {}", n.address());
    let ip = node.http_api_address();
    let p = node.http_api_port();
    println!("REST API available at: http://{ip}:{p}");
    println!("REST API Put and Get available at: http://{ip}:{p}/data");

    Ok((cluster, node))
}

fn init_testmode_logger(loglevel: log::LevelFilter) {
    let stdout = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new( "{d(%Y-%m-%d %H:%M:%S):<20} {M:>20.30}:{L:>3} {h({l})}    {m}\n")))
        .build();

    let _20mb = 20000000;
    let trigger = SizeTrigger::new(_20mb);
    let roller = FixedWindowRoller::builder()
        .build( "./log/archive/info.{}.log", 10).unwrap();
    let policy = CompoundPolicy::new(Box::new(trigger), Box::new(roller));
    let requests_error = RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d(%Y-%m-%d %H:%M:%S):<20} {M:>20.30}:{L:>3} {l} {m}{n}")))
        .build( "./log/error.log", Box::new(policy)).unwrap();

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .appender(Appender::builder().build("requests_error", Box::new(requests_error)))
        .logger(Logger::builder()
                .appender("stdout")
                .appender("requests_error")
                .additive(false)
                .build("bob", loglevel))
        .logger(Logger::builder()
                .appender("stdout")
                .appender("requests_error")
                .additive(false)
                .build("bob_backend", loglevel))
        .logger(Logger::builder()
                .appender("stdout")
                .appender("requests_error")
                .additive(false)
                .build("bob_common", loglevel))
        .logger(Logger::builder()
                .appender("stdout")
                .appender("requests_error")
                .additive(false)
                .build("bob_apps", loglevel))
        .logger(Logger::builder()
                .appender("stdout")
                .appender("requests_error")
                .additive(false)
                .build("bob_grpc", loglevel))
        .logger(Logger::builder()
                .appender("stdout")
                .appender("requests_error")
                .additive(false)
                .build("pearl", loglevel))
        .build(Root::builder().appender("stdout").build(loglevel))
        .unwrap();
    log4rs::init_config(config).unwrap();
}

async fn run_server<A: Authenticator>(node: NodeConfig, authenticator: A, mapper: VirtualMapper, address: IpAddr, port: u16, addr: SocketAddr) {
    let (metrics, shared_metrics) = init_counters(&node, &addr.to_string()).await;
    let handle = Handle::current();
    let factory = Factory::new(node.operation_timeout(), metrics, node.name().into());

    let bob = BobServer::new(
        Grinder::new(mapper, &node).await,
        handle,
        shared_metrics,
        authenticator,
    );
    info!("Start backend");
    bob.run_backend().await.unwrap();
    create_signal_handlers(&bob).unwrap();
    bob.run_periodic_tasks(factory);
    bob.run_api_server(address, port);

    let bob_service = BobApiServer::new(bob);
    Server::builder()
        .tcp_nodelay(true)
        .add_service(bob_service)
        .serve(addr)
        .await
        .unwrap();
}

async fn nodes_credentials_from_cluster_config(
    cluster_config: &ClusterConfig,
) -> HashMap<IpAddr, Credentials> {
    let mut nodes_creds = HashMap::new();
    for node in cluster_config.nodes() {
        let address = &node.address();
        let address = if let Ok(address) = address.parse() {
            address
        } else {
            match lookup_host(address).await {
                Ok(mut address) => address
                    .next()
                    .expect("failed to resolve hostname: dns returned empty ip list"),
                Err(e) => {
                    error!("expected SocketAddr/hostname, found: {}", address);
                    error!("{}", e);
                    panic!("failed to resolve hostname")
                }
            }
        };
        let creds = Credentials::builder()
            .with_nodename(node.name())
            .with_address(Some(address))
            .build();
        nodes_creds.insert(creds.ip().expect("node missing ip"), creds);
    }
    nodes_creds
}

fn bind_all_interfaces(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port)
}

fn port_from_address(addr: &str) -> Option<u16> {
    addr.rsplit_once(':')
        .and_then(|(_, port)| port.parse::<u16>().ok())
}

fn create_signal_handlers<A: Authenticator>(
    server: &BobServer<A>,
) -> Result<(), Box<dyn ErrorTrait>> {
    let signals = [SignalKind::terminate(), SignalKind::interrupt()];
    for s in signals.iter() {
        spawn_signal_handler(server, *s)?;
    }
    Ok(())
}

fn spawn_signal_handler<A: Authenticator>(
    server: &BobServer<A>,
    s: tokio::signal::unix::SignalKind,
) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::signal::unix::signal;
    let mut task = signal(s)?;
    let server = server.clone();
    tokio::spawn(async move {
        task.recv().await;
        debug!("Got signal {:?}", s);
        server.shutdown().await;
        log::logger().flush();
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
    let ver = format!("{}\n{}", crate_version!(), BuildInfo::default());
    let testmode_sc = SubCommand::with_name("testmode")
        .about("Bob's test mode")
        .arg(
            Arg::with_name("data")
            .help("Path to bob data directory")
            .takes_value(true)
            .long("data")
        )
        .arg(
            Arg::with_name("grpc-port")
            .help("gRPC API port")
            .takes_value(true)
            .long("grpc-port")
        )
        .arg(
            Arg::with_name("restapi-port")
            .help("REST API port")
            .takes_value(true)
            .long("restapi-port")
        )
        .arg(
            Arg::with_name("nodes")
            .help("Comma separated node addresses. Example: 127.0.0.1:20000,127.0.0.1:20001")
            .takes_value(true)
            .long("nodes")
        );

    App::new("bobd")
        .version(ver.as_str())
        .arg(
            Arg::with_name("cluster")
                .help("Cluster config file")
                .takes_value(true)
                .short("c")
                .long("cluster"),
        )
        .arg(
            Arg::with_name("node")
                .help("Node config file")
                .takes_value(true)
                .short("n")
                .long("node"),
        )
        .arg(
            Arg::with_name("name")
                .help("Node name")
                .takes_value(true)
                .short("a")
                .long("name"),
        )
        .arg(
            Arg::with_name("http_api_address")
                .help("Http api address")
                .short("h")
                .long("host")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("http_api_port")
                .help("Http api port")
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
        .subcommand(testmode_sc)
        .get_matches()
}
