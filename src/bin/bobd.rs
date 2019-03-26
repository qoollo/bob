use bob::api::grpc::{server, PutRequest, GetRequest, OpStatus, BlobResult};

use futures::{future, Future, Stream};
use tokio::executor::DefaultExecutor;
use tokio::net::TcpListener;
use tower_h2::Server;
use tower_grpc::{Request, Response};

#[derive(Clone, Debug)]
struct BobSrv;

impl server::BobApi for BobSrv {
        type PutFuture = future::FutureResult<Response<OpStatus>, tower_grpc::Status>;
        type GetFuture = future::FutureResult<Response<BlobResult>, tower_grpc::Status>;

        fn put(&mut self, _request: Request<PutRequest>) -> Self::PutFuture {
            let response = Response::new(OpStatus {
                error: None
            });
            future::ok(response)
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


    let new_service = server::BobApiServer::new(BobSrv);

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
