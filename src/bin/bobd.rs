use std::thread;

use grpc::RequestOptions;
use grpc::SingleResponse;
use grpc::ServerBuilder;

use futures::future::*;   

use bob::api::bob_grpc::{BobApi, BobApiServer};
use bob::api::bob::{PutRequest, GetRequest, OpStatus, BlobResult};
use bob::core::grinder::{Grinder};
use bob::core::data::{BobKey, BobData, BobOptions};
use bob::core::backend::Backend;
use bob::core::sprinkler::{Sprinkler};


struct BobApiImpl {
    grinder: Grinder
}

impl BobApi for BobApiImpl {
    fn put(&self, _ :RequestOptions, req:PutRequest) -> SingleResponse<OpStatus> {
        let f = self.grinder.put(
            BobKey{
                key: req.get_key().get_key()
                },
            BobData{
                data: req.get_data().get_data().to_vec()
            },
            {
                let mut opts: BobOptions = Default::default();
                if req.get_options().get_force_node() {
                    opts = opts | BobOptions::FORCE_NODE;
                }
                opts
            }
        ).then(|_r| ok(OpStatus::new())); 
        SingleResponse::no_metadata(Box::new(f))
        //SingleResponse::no_metadata(op)      
    }
    fn get(&self, _:RequestOptions, _:GetRequest) -> SingleResponse<BlobResult> {
        SingleResponse::completed(BlobResult::new())
    }
}

fn main() {
    let port = 20000;
    let mut server = ServerBuilder::new_plain();
    server.http.set_port(port);
    server.add_service(BobApiServer::new_service_def(BobApiImpl{
        grinder: Grinder {
            backend: Backend {},
            sprinkler: Sprinkler::new()
        },
    }));

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
