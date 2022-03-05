use bob::{
    build_info::BuildInfo, init_counters, BobApiServer, BobServer, ClusterConfig, Factory, Grinder,
    VirtualMapper,
};
use bob_access::{Authenticator, BasicAuthenticator, Credentials, StubAuthenticator, UsersMap};
use clap::{crate_version, App, Arg, ArgMatches};
use std::{
    collections::HashMap,
    error::Error as ErrorTrait,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    io::{Read, BufReader},
    fs::File,
};
use tokio::{net::lookup_host, runtime::Handle, signal::unix::SignalKind};
use tonic::transport::{Server, ServerTlsConfig, Identity};

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

    log4rs::init_file(node.log_config(), Default::default()).expect("can't find log config");

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

    info!("Start API server");
    let http_api_port = matches
        .value_of("http_api_port")
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| node.http_api_port());
    let http_api_address = matches
        .value_of("http_api_address")
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| node.http_api_address());

    let factory = if node.tls() {
        if let Some(node_tls_config) = node.tls_config() {
            let ca_cert_path = node_tls_config.ca_cert_path.clone();
            Factory::new(node.operation_timeout(), metrics, Some(ca_cert_path))
        } else {
            error!("tls enabed, but not specified, add \"tls:\" to node config");
            panic!("tls enabed, but not specified, add \"tls:\" to node config");
        }
    } else {
        Factory::new(node.operation_timeout(), metrics, None)
    };
    let grinder = Grinder::new(mapper.clone(), &node).await;
    let authentication_type = matches.value_of("authentication_type").unwrap();

    let mut server_builder = Server::builder();

    if node.tls() {
        if let Some(node_tls_config) = node.tls_config() {
            let cert_path = node_tls_config.cert_path.as_ref().expect("no certificate path specified");
            let cert_bin = load_tls_certificate(cert_path);
            let pkey_path = node_tls_config.pkey_path.as_ref().expect("no private key path specified");
            let key_bin = load_tls_pkey(pkey_path);
            let identity = Identity::from_pem(cert_bin.clone(), key_bin);
            
            let tls_config = ServerTlsConfig::new().identity(identity);
            server_builder = server_builder.tls_config(tls_config).expect("grpc tls config");
        } else {
            error!("tls enabed, but not specified, add \"tls:\" to node config");
            panic!("tls enabed, but not specified, add \"tls:\" to node config");
        }
    }

    match authentication_type {
        "stub" => {
            let users_storage =
                UsersMap::from_file(node.users_config()).expect("Can't parse users and roles");
            let authenticator = StubAuthenticator::new(users_storage);
            let bob = BobServer::new(grinder, handle, shared_metrics, authenticator);
            info!("Start backend");
            bob.run_backend().await.unwrap();
            create_signal_handlers(&bob).unwrap();
            bob.run_periodic_tasks(factory);
            bob.run_api_server(http_api_address, http_api_port);

            let bob_service = BobApiServer::new(bob);
            server_builder
                .tcp_nodelay(true)
                .add_service(bob_service)
                .serve(addr)
                .await
                .unwrap();
        }
        "basic" => {
            let users_storage =
                UsersMap::from_file(node.users_config()).expect("Can't parse users and roles");
            let mut authenticator = BasicAuthenticator::new(users_storage);
            let nodes_credentials = nodes_credentials_from_cluster_config(&cluster).await;
            authenticator
                .set_nodes_credentials(nodes_credentials)
                .expect("failed to gen nodes credentials from cluster config");
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
            bob.run_api_server(http_api_address, http_api_port);

            

            let bob_service = BobApiServer::new(bob);
            server_builder
                .tcp_nodelay(true)
                .add_service(bob_service)
                .serve(addr)
                .await
                .unwrap();
        }
        _ => {
            warn!("valid authentication type not provided");
        }
    }
}

fn load_tls_certificate(path: &str) -> Vec<u8> {
    let f = File::open(path).expect("can not open tls certificate file");
    let mut reader = BufReader::new(f);
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).expect("can not read tls certificate from file");
    buffer
}

fn load_tls_pkey(path: &str) -> Vec<u8> {
    let f = File::open(path).expect("can not open tls private key file");
    let mut reader = BufReader::new(f);
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).expect("can not read tls private key from file");
    buffer
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
            .with_username_password(node.name(), "")
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
        std::process::exit(0);
    });
    Ok(())
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
            Arg::with_name("authentication_type")
                .default_value("stub")
                .long("auth")
                .possible_values(&["stub", "basic"])
                .takes_value(true),
        )
        .get_matches()
}
