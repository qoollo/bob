include!("alloc/mimalloc.rs");

use bob::{
    build_info::BuildInfo, init_counters, BobApiServer, BobServer, ClusterConfig, NodeConfig, Factory, Grinder,
    VirtualMapper, BackendType, FactoryTlsConfig,
};
use bob_access::{Authenticator, BasicAuthenticator, DeclaredCredentials, StubAuthenticator, UsersMap, AuthenticationType};
use clap::{crate_version, App, Arg, ArgMatches};
use std::{
    collections::HashMap,
    error::Error as ErrorTrait,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
use tokio::runtime::Handle;
use tonic::transport::Server;
use qoollo_log4rs_logstash::config::DeserializersExt; 
use std::path::PathBuf;
use std::fs::create_dir;

#[macro_use]
extern crate log;

#[tokio::main]
async fn main() {
    let matches = get_matches();

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
    let cluster = ClusterConfig::try_get(cluster_config).await.unwrap();

    let node_config_file = matches.value_of("node").unwrap();
    println!("Node config: {:?}", node_config_file);
    let node = cluster.get(node_config_file).await.unwrap();

    let mut extra_logstash_fields = HashMap::new();
    extra_logstash_fields.insert("node_name".to_string(), serde_json::Value::String(node.name().to_string()));
    if let Some(cluster_node_info) = cluster.nodes().iter().find(|item| item.name() == node.name()) {
        extra_logstash_fields.insert("node_address".to_string(), serde_json::Value::String(cluster_node_info.address().to_string()));
    }
    log4rs::init_file(node.log_config(), log4rs::config::Deserializers::default().with_logstash_extra(extra_logstash_fields))
        .expect("can't find log config");

    check_folders(&node, matches.is_present("init_folders"));

    let mut mapper = VirtualMapper::new(&node, &cluster);

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
        mapper = VirtualMapper::new(&node, &cluster);
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
            let mut authenticator = BasicAuthenticator::new(users_storage, node.hostname_resolve_period_ms());
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

async fn run_server<A: Authenticator>(node: NodeConfig, authenticator: A, mapper: VirtualMapper, address: IpAddr, port: u16, addr: SocketAddr) {
    let (metrics, shared_metrics) = init_counters(&node, &addr.to_string()).await;
    let handle = Handle::current();
    let factory_tls_config = node.tls_config().as_ref().and_then(|tls_config| tls_config.grpc_config())
        .map(|tls_config| {
            let ca_cert = std::fs::read(&tls_config.ca_cert_path).expect("can not read ca certificate from file");
            FactoryTlsConfig {
                ca_cert,
                tls_domain_name: tls_config.domain_name.clone(),
            }
        });
    let factory = Factory::new(node.operation_timeout(), metrics, node.name().into(), factory_tls_config);

    let mut server_builder = Server::builder();
    if let Some(node_tls_config) = node.tls_config().as_ref().and_then(|tls_config| tls_config.grpc_config()) {
        let tls_config = node_tls_config.to_server_tls_config();
        server_builder = server_builder.tls_config(tls_config).expect("grpc tls config");
    }

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
    bob.run_api_server(address, port, node.tls_config()).await;

    let bob_service = BobApiServer::new(bob);
    server_builder
        .tcp_nodelay(true)
        .add_service(bob_service)
        .serve(addr)
        .await
        .unwrap();
}

async fn nodes_credentials_from_cluster_config(
    cluster_config: &ClusterConfig,
) -> HashMap<String, DeclaredCredentials> {
    let mut nodes_creds: HashMap<String, DeclaredCredentials> = HashMap::new();
    for node in cluster_config.nodes() {
        let address = node.address();
        let cred = 
        if let Ok(address) = address.parse::<SocketAddr>() {
            DeclaredCredentials::internode_builder(node.name())
                .with_address(address)
                .build()
        } else {
            DeclaredCredentials::internode_builder(node.name())
                .with_hostname(address.into())
                .build()
        };
        nodes_creds.insert(node.name().into(), cred);
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

#[cfg(target_family = "unix")]
fn create_signal_handlers<A: Authenticator>(server: &BobServer<A>) -> Result<(), Box<dyn ErrorTrait>> {
    use tokio::signal::unix::{*};
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    spawn_signal_handler(server, async move {
        tokio::select! {
            _ = terminate.recv() => "terminate",
            _ = interrupt.recv() => "interrupt"
        }
    });
    Ok(())
}

#[cfg(target_family = "windows")]
fn create_signal_handlers<A: Authenticator>(server: &BobServer<A>) -> Result<(), Box<dyn ErrorTrait>> {
    use tokio::signal::windows::{*};
    let mut ctrl_c = ctrl_c()?;
    spawn_signal_handler(server, async move {
        tokio::select! {
            _ = ctrl_c.recv() => "ctrl_c"
        }
    });
    Ok(())
}

fn spawn_signal_handler<A: Authenticator, TFut: futures::Future<Output = &'static str> + Send + 'static>(
    server: &BobServer<A>,
    signal_tasks_future: TFut
) {
    let server = server.clone();
    tokio::spawn(async move {
        let signal_name = signal_tasks_future.await;
        info!("Got signal '{}'. Shutdown started", signal_name);
        server.shutdown().await;
        log::logger().flush();
        std::process::exit(0);
    });
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
