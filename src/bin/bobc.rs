use grpc::ClientStubExt;
use grpc::RequestOptions;

use bob::api::bob_grpc::{BobApi, BobApiClient};
use bob::api::bob::{PutRequest, GetRequest, OpStatus, BlobResult};

fn main() {
    let port = 20000;
    let client = BobApiClient::new_plain("::1", port, Default::default()).unwrap();
    let put_req = PutRequest::new();
    let put_resp = client.put(RequestOptions::new(), put_req);

    println!("PUT: {:?}", put_resp.wait());
    let get_req = GetRequest::new();
    let get_resp = client.get(RequestOptions::new(), get_req);

    println!("GET: {:?}", get_resp.wait());
}