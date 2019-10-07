use std::io;

use bob::api::grpc::client::BobApi;
use bob::api::grpc::{Blob, BlobKey, BlobMeta, GetRequest, PutRequest};

use tokio::runtime::Runtime;

use futures::Future;
use hyper::client::connect::{Destination, HttpConnector};
use std::time::{SystemTime, UNIX_EPOCH};
use tower_hyper::{client, util};

fn wait_for_input() {
    println!("Press any key to send GET");

    io::stdin().read_line(&mut String::default()).unwrap();
}

fn main() {
    let mut rt = Runtime::new().unwrap();
    let uri: http::Uri = "http://localhost:20000".parse().unwrap();

    let dst = Destination::try_from_uri(uri.clone()).unwrap();
    let connector = util::Connector::new(HttpConnector::new(4));
    let settings = client::Builder::new().http2_only(true).clone();
    let mut make_client = client::Connect::with_builder(connector, settings);

    let conn_l = rt.block_on(make_client.make_service(dst)).unwrap();
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
                meta: Some(BlobMeta {
                    timestamp: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("msg: &str")
                        .as_secs() as i64,
                }),
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
            key: Some(BlobKey { key: 1 }),
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
