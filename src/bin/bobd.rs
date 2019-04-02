use bob::api::grpc::{server, BlobResult, BobError, GetRequest, Null, OpStatus, PutRequest};

use bob::core::backend::Backend;
use bob::core::data::{BobData, BobKey, BobOptions};
use bob::core::grinder::Grinder;
use bob::core::sprinkler::Sprinkler;
use clap::{App, Arg};
use env_logger;
use futures::future::ok;
use futures::{future, Future, Stream};
use log::LevelFilter;
use stopwatch::Stopwatch;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tower_grpc::{Request, Response};
use tower_h2::Server;
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

    fn put_validate(&self, req: &PutRequest) -> bool {
        !(req.key == None || req.data == None)
    }
}

impl server::BobApi for BobSrv {
    type PutFuture = Box<Future<Item = Response<OpStatus>, Error = tower_grpc::Status> + Send>;
    type GetFuture = future::FutureResult<Response<BlobResult>, tower_grpc::Status>;
    type PingFuture = future::FutureResult<Response<Null>, tower_grpc::Status>;

    fn put(&mut self, req: Request<PutRequest>) -> Self::PutFuture {
        let sw = Stopwatch::start_new();
        let param = req.into_inner();

        if !self.put_validate(&param) {
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
                                info!(
                                    "PUT[{}]-OK local:{} ok dt: {}ms",
                                    key, !r_ok.is_clustered, elapsed
                                );
                                ok(Response::new(OpStatus { error: None }))
                            }
                            Err(r_err) => {
                                error!(
                                    "PUT[{}]-ERR local:{}  dt: {}ms {}",
                                    key,
                                    !r_err.is_clustered,
                                    elapsed,
                                    r_err.to_string()
                                );
                                ok(Response::new(OpStatus {
                                    error: Some(BobError {
                                        code: 1,
                                        desc: format!("Failed to write {}", r_err.to_string()),
                                    }),
                                }))
                            }
                        }
                    }),
            )
        }
    }

    fn get(&mut self, _request: Request<GetRequest>) -> Self::GetFuture {
        let response = Response::new(BlobResult {
            status: None,
            data: None,
        });
        future::ok(response)
    }

    fn ping(&mut self, _request: Request<Null>) -> Self::PingFuture {
        debug!("PING");
        let response = Response::new(Null {});
        future::ok(response)
    }
}

fn main() {
    env_logger::builder()
        .filter_module("bob", LevelFilter::Info)
        .init();

    let matches = App::new("Bob")
        .arg(
            Arg::with_name("bind")
                .help("server bind point")
                .takes_value(true)
                .short("b")
                .long("bind")
                .default_value("127.0.0.1:20000"),
        )
        .get_matches();

    let mut rt = Runtime::new().unwrap();

    let bob = BobSrv {
        grinder: std::sync::Arc::new(Grinder {
            backend: Backend {},
            sprinkler: Sprinkler::new(),
        }),
    };

    rt.spawn(bob.get_periodic_tasks(rt.executor()));
    let new_service = server::BobApiServer::new(bob);

    let h2_settings = Default::default();
    let mut h2 = Server::new(new_service, h2_settings, rt.executor());

    let addr = matches
        .value_of("bind")
        .unwrap_or_default()
        .parse()
        .unwrap();
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
