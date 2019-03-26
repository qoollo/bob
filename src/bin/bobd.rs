use bob::api::grpc::{server, PutRequest, GetRequest, OpStatus, BlobResult};

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


#[derive(Clone)]
struct BobSrv {
    grinder: Grinder
}

impl server::BobApi for BobSrv {
        type PutFuture = Box<Future<Item=Response<OpStatus>,Error=tower_grpc::Status> + Send>;
        type GetFuture = future::FutureResult<Response<BlobResult>, tower_grpc::Status>;

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
}

fn main() {


    let new_service = server::BobApiServer::new(BobSrv {
        grinder: Grinder {
            backend: Backend {},
            sprinkler: Sprinkler::new()
        }
    });

    let h2_settings = Default::default();
    let mut h2 = Server::new(new_service, h2_settings, DefaultExecutor::current());

    let addr = "127.0.0.1:20000".parse().unwrap();
    let bind = TcpListener::bind(&addr).expect("bind");

    let serve = bind.incoming()
        .for_each(move |sock| {
            if let Err(e) = sock.set_nodelay(true) {
                return Err(e);
            }

            let serve = h2.serve(sock);
            tokio::spawn(serve.map_err(|e| println!("h2 error: {:?}", e)));

            Ok(())
        })
        .map_err(|e| eprintln!("accept error: {}", e));

    tokio::run(serve)
}
