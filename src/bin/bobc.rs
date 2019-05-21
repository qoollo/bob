use futures::{Future, Poll};
use std::io;
use tokio::net::tcp::{ConnectFuture, TcpStream};
use tower::MakeService;
use tower_grpc::Request;
use tower_h2::client;
use tower_service::Service;

use bob::api::grpc::client::BobApi;
use bob::api::grpc::{Blob, BlobKey, BlobMeta, GetRequest, PutRequest};

use tokio::runtime::Runtime;

fn wait_for_input() {
    println!("Press any key to send GET");

    io::stdin().read_line(&mut String::default()).unwrap();
}

fn main() {
    let mut rt = Runtime::new().unwrap();
    let uri: http::Uri = "http://localhost:20000".parse().unwrap();

    let h2_settings = Default::default();
    let mut make_client = client::Connect::new(Dst, h2_settings, rt.executor());

    let conn_l = rt.block_on(make_client.make_service(())).unwrap();
    let conn = tower_request_modifier::Builder::new()
        .set_origin(uri)
        .build(conn_l)
        .unwrap();
    let mut client = BobApi::new(conn);

    let pur_req = client
        .put(Request::new(PutRequest {
            key: Some(BlobKey { key: 0 }),
            data: Some(Blob {
                data: vec![0],
                meta: Some(BlobMeta { timestamp: 1 }),
            }), // TODO
            options: None,
        }))
        .map_err(|e| println!("gRPC request failed; err={:?}", e))
        .and_then(|response| {
            println!("RESPONSE = {:?}", response);
            Ok(())
        })
        .map_err(|e| {
            println!("ERR = {:?}", e);
        });

    rt.block_on(pur_req).unwrap();
    wait_for_input();
    let get_req = client
        .get(Request::new(GetRequest {
            key: Some(BlobKey { key: 0 }),
            options: None,
        }))
        .map_err(|e| println!("gRPC request failed; err={:?}", e))
        .and_then(|response| {
            println!("RESPONSE = {:?}", response);
            Ok(())
        })
        .map_err(|e| {
            println!("ERR = {:?}", e);
        });

    rt.block_on(get_req).unwrap();

    wait_for_input();
    let get_req2 = client
        .get(Request::new(GetRequest {
            key: None,
            options: None,
        }))
        .map_err(|e| println!("gRPC request failed; err={:?}", e))
        .and_then(|response| {
            println!("RESPONSE = {:?}", response);
            Ok(())
        })
        .map_err(|e| {
            println!("ERR = {:?}", e);
        });

    rt.block_on(get_req2).unwrap();
    // println!("PUT: {:?}", put_resp.wait());
    // let get_req = GetRequest::new();
    // let get_resp = client.get(RequestOptions::new(), get_req);

    // println!("GET: {:?}", get_resp.wait());
}

struct Dst;

impl Service<()> for Dst {
    type Response = TcpStream;
    type Error = ::std::io::Error;
    type Future = ConnectFuture;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(().into())
    }

    fn call(&mut self, _: ()) -> Self::Future {
        TcpStream::connect(&([127, 0, 0, 1], 20000).into())
    }
}
