use bob::api::grpc::{server, Blob, GetRequest, Null, OpStatus, PutRequest};

use bob::core::backend::BackendError;

use bob::core::data::{BobData, BobError, BobKey, BobOptions, VDiskMapper};
use bob::core::grinder::{Grinder, ServeTypeError, ServeTypeOk};
use clap::{App, Arg};
use env_logger;
use futures::future::{err, ok};
use futures::{future, Future, Stream};
use stopwatch::Stopwatch;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tower_grpc::{Request, Response};
use tower_h2::Server;

use bob::core::configs::cluster::{BobClusterConfig, ClusterConfigYaml};
use bob::core::configs::node::{BobNodeConfig, NodeConfigYaml};

#[macro_use]
extern crate log;

#[derive(Clone)]
struct BobSrv {
    grinder: std::sync::Arc<Grinder>,
}

impl BobSrv {
    pub fn get_periodic_tasks(
        &self,
        ex: tokio::runtime::TaskExecutor,
    ) -> Box<impl Future<Item = (), Error = ()>> {
        self.grinder.get_periodic_tasks(ex)
    }

    fn put_is_valid(req: &PutRequest) -> bool {
        req.key.is_some() && req.data.is_some()
    }

    fn get_is_valid(req: &GetRequest) -> bool {
        req.key.is_some()
    }
}

impl server::BobApi for BobSrv {
    type PutFuture = Box<Future<Item = Response<OpStatus>, Error = tower_grpc::Status> + Send>;
    type GetFuture = Box<Future<Item = Response<Blob>, Error = tower_grpc::Status> + Send>;
    type PingFuture = future::FutureResult<Response<Null>, tower_grpc::Status>;

    fn put(&mut self, req: Request<PutRequest>) -> Self::PutFuture {
        let sw = Stopwatch::start_new();
        let param = req.into_inner();

        if !Self::put_is_valid(&param) {
            warn!("PUT[-] invalid arguments - key and data is mandatory");
            Box::new(future::err(tower_grpc::Status::new(
                tower_grpc::Code::InvalidArgument,
                "Key and data is mandatory",
            )))
        } else {
            let key = BobKey {
                key: param.key.unwrap().key,
            };
            let data = BobData {
                data: param.data.unwrap().data,
            };

            trace!("PUT[{}] data size: {}", key, data.data.len());
            Box::new(
                self.grinder
                    .put(key, data, {
                        let mut opts: BobOptions = Default::default();
                        if let Some(vopts) = param.options.as_ref() {
                            if vopts.force_node {
                                opts |= BobOptions::FORCE_NODE;
                            }
                        }
                        opts
                    })
                    .then(move |r| {
                        let elapsed = sw.elapsed_ms();
                        match r {
                            Ok(r_ok) => {
                                debug!("PUT[{}]-OK local:{:?} ok dt: {}ms", key, r_ok, elapsed);
                                ok(Response::new(OpStatus { error: None }))
                            }
                            Err(r_err) => {
                                error!("PUT[{}]-ERR dt: {}ms {:?}", key, elapsed, r_err);
                                err(tower_grpc::Status::new(
                                    tower_grpc::Code::Internal,
                                    format!("Failed to write {:?}", r_err),
                                ))
                            }
                        }
                    }),
            )
        }
    }

    fn get(&mut self, req: Request<GetRequest>) -> Self::GetFuture {
        let sw = Stopwatch::start_new();
        let param = req.into_inner();
        if !Self::get_is_valid(&param) {
            warn!("GET[-] invalid arguments - key is mandatory");
            Box::new(future::err(tower_grpc::Status::new(
                tower_grpc::Code::InvalidArgument,
                "Key is mandatory",
            )))
        } else {
            let key = BobKey {
                key: param.key.unwrap().key,
            };

            Box::new(
                self.grinder
                    .get(key, {
                        let mut opts: BobOptions = Default::default();
                        if let Some(vopts) = param.options.as_ref() {
                            if vopts.force_node {
                                opts |= BobOptions::FORCE_NODE;
                            }
                        }
                        opts
                    })
                    .then(move |r| {
                        let elapsed = sw.elapsed_ms();
                        match r {
                            Ok(r_ok) => {
                                debug!(
                                    "GET[{}]-OK local:{} dt: {}ms",
                                    key,
                                    r_ok.is_local(),
                                    elapsed
                                );
                                ok(Response::new(Blob {
                                    data: match r_ok {
                                        ServeTypeOk::Cluster(r) => r.result.data.data,
                                        ServeTypeOk::Local(r) => r.data.data,
                                    },
                                }))
                            }
                            Err(r_err) => {
                                error!(
                                    "GET[{}]-ERR local:{}  dt: {}ms {:?}",
                                    key,
                                    r_err.is_local(),
                                    elapsed,
                                    r_err
                                );
                                let err = match r_err {
                                    ServeTypeError::Cluster(cerr) => match cerr {
                                        BobError::NotFound => tower_grpc::Status::new(
                                            tower_grpc::Code::NotFound,
                                            format!("[cluster] Can't find blob with key {}", key),
                                        ),
                                        _ => tower_grpc::Status::new(
                                            tower_grpc::Code::Unknown,
                                            "[cluster]Some error",
                                        ),
                                    },
                                    ServeTypeError::Local(lerr) => match lerr {
                                        BackendError::NotFound => tower_grpc::Status::new(
                                            tower_grpc::Code::NotFound,
                                            format!("[backend] Can't find blob with key {}", key),
                                        ),
                                        _ => tower_grpc::Status::new(
                                            tower_grpc::Code::Unknown,
                                            "[backend] Some error",
                                        ),
                                    },
                                };
                                future::err(err)
                            }
                        }
                    }),
            )
        }
    }

    fn ping(&mut self, _request: Request<Null>) -> Self::PingFuture {
        debug!("PING");
        let response = Response::new(Null {});
        future::ok(response)
    }
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

    let mapper = VDiskMapper::new(disks.to_vec(), &node);
    let bob = BobSrv {
        grinder: std::sync::Arc::new(Grinder::new(mapper, &node)),
    };

    rt.spawn(bob.get_periodic_tasks(rt.executor()));
    let new_service = server::BobApiServer::new(bob);

    let h2_settings = Default::default();
    let mut h2 = Server::new(new_service, h2_settings, rt.executor());

    let addr = node.bind().parse().unwrap();
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
