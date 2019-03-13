use std::thread;

use grpc::RequestOptions;
use grpc::SingleResponse;
use grpc::ServerBuilder;

use bob::api::bob_grpc::{BobApi, BobApiServer};
use bob::api::bob::{PutRequest, GetRequest, OpStatus, BlobResult};

struct BobApiImpl;
impl BobApi for BobApiImpl {
    fn put(&self, _ :RequestOptions, _:PutRequest) -> SingleResponse<OpStatus> {
        
        SingleResponse::completed(OpStatus::new())
    }
    fn get(&self, _:RequestOptions, _:GetRequest) -> SingleResponse<BlobResult> {
        SingleResponse::completed(BlobResult::new())
    }
}

fn main() {
    let port = 20000;
    let mut server = ServerBuilder::new_plain();
    server.http.set_port(port);
    server.add_service(BobApiServer::new_service_def(BobApiImpl));

    server.http.set_cpu_pool_threads(4);
    let _server = server.build().expect("server");
    println!(
        "bobd started on port {}",
        port
    );

    loop {
        thread::park();
    }
}
