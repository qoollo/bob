use bob::api::grpc::{server, PutRequest, GetRequest, OpStatus, BlobResult, Null};

use futures::{future, Future, Stream};
use futures::future::ok;
use tokio::executor::DefaultExecutor;
use tokio::net::TcpListener;
use tower_h2::Server;
use tower_grpc::{Request, Response};
use bob::core::grinder::{Grinder};
use bob::core::data::{BobKey, BobData, BobOptions};
use bob::core::backend::Backend;
use bob::core::sprinkler::{Sprinkler};
use tokio::runtime::{Runtime};
extern crate clap;

use clap::{App, Arg};

#[derive(Clone)]
struct BobSrv {
    grinder: Grinder
}

impl BobSrv {
    pub fn get_periodic_tasks(&self, ex: tokio::runtime::TaskExecutor) -> Box<impl Future<Item=(), Error=()>> {
        self.grinder.get_periodic_tasks(ex)
    }
}

impl server::BobApi for BobSrv {
        type PutFuture = Box<Future<Item=Response<OpStatus>,Error=tower_grpc::Status> + Send>;
        type GetFuture = future::FutureResult<Response<BlobResult>, tower_grpc::Status>;
        type PingFuture = future::FutureResult<Response<Null>, tower_grpc::Status>;

        fn put(&mut self, req: Request<PutRequest>) -> Self::PutFuture {
            let param = req.get_ref();
            let f = self.grinder.put(
                BobKey{
                    key: param.key.as_ref().unwrap().key.clone()
                    },
                BobData{
                    data: param.data.as_ref().unwrap().data.clone()
                },
                {
                    let mut opts: BobOptions = Default::default();
                    match param.options.as_ref() {
                        Some(vopts) => { 
                            if vopts.force_node {
                                opts = opts | BobOptions::FORCE_NODE;
                            }
                        },
                        None => {}
                    }
                    opts
                }
            ).then(|_r| ok(Response::new(OpStatus {
                error: None
            })));//.map_err(|_| err(tower_grpc::Status::new(tower_grpc::Code::Unknown, "error")));
            Box::new(f)
        }

        fn get(&mut self, _request: Request<GetRequest>) -> Self::GetFuture{
            let response = Response::new(BlobResult {
                status: None,
                data: None
            });
            future::ok(response)
        }

        fn ping(&mut self, _request: Request<Null>) -> Self::PingFuture{
            let response = Response::new(Null { });
            future::ok(response)
        }
}

fn main() {

    let matches = App::new("Bob")
                    .arg(Arg::with_name("bind")
                                    .help("server bind point")
                                    .takes_value(true)
                                    .short("b")
                                    .long("bind")
                                    .default_value("127.0.0.1:20000"))
                    .get_matches();

    let mut rt = Runtime::new().unwrap();

    let bob = BobSrv {
        grinder: Grinder {
            backend: Backend {},
            sprinkler: Sprinkler::new()
        }
    };
    
    rt.spawn(bob.get_periodic_tasks(rt.executor()));
    let new_service = server::BobApiServer::new(bob);

    let h2_settings = Default::default();
    let mut h2 = Server::new(new_service, h2_settings, DefaultExecutor::current());

    let addr = matches.value_of("bind").unwrap_or_default().parse().unwrap();
    println!("Listen on {:?}", addr);
    let bind = TcpListener::bind(&addr).expect("bind");

    let serve = bind.incoming()
        .for_each(move |sock| {
            if let Err(e) = sock.set_nodelay(true) {
                return Err(e);
            }

            let serve = h2.serve(sock);
            tokio::spawn(serve.map_err(|e| println!("Server h2 error: {:?}", e)));

            Ok(())
        })
        .map_err(|e| eprintln!("accept error: {}", e));

    rt.block_on_all(serve);
}
