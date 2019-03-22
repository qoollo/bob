use grpc::ClientStubExt;
use grpc::RequestOptions;

use std::io;
use std::io::prelude::*;

use bob::api::bob_grpc::{BobApi, BobApiClient};
use bob::api::bob::{PutRequest, GetRequest};

fn main() {
    let mut stdin = io::stdin();
    let port = 20000;
    let client = BobApiClient::new_plain("127.0.0.1", port, Default::default()).unwrap();
    let put_req = PutRequest::new();
    println!("Will put and get.");
    let put_resp = client.put(RequestOptions::new(), put_req);
    println!("PUT: {:?}", put_resp.wait());

    let _ = stdin.read(&mut [0u8]).unwrap();
    let get_req = GetRequest::new();
    let get_resp = client.get(RequestOptions::new(), get_req);
    println!("GET: {:?}", get_resp.wait());
    
    let _ = stdin.read(&mut [0u8]).unwrap();
    let get_req = GetRequest::new();
    let get_resp = client.get(RequestOptions::new(), get_req);
    println!("GET: {:?}", get_resp.wait());
}