use bob::grpc::bob_api_client::BobApiClient;
use bob::grpc::{Blob, BlobKey, BlobMeta, GetRequest, PutRequest};
use std::io;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::Request;

fn wait_for_input() {
    println!("Press any key to send GET");

    io::stdin().read_line(&mut String::default()).unwrap();
}

#[tokio::main]
async fn main() {
    let mut client = BobApiClient::connect("http://localhost:20000")
        .await
        .unwrap();

    let put_req = Request::new(PutRequest {
        key: Some(BlobKey { key: 0 }),
        data: Some(Blob {
            data: vec![0],
            meta: Some(BlobMeta {
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("msg: &str")
                    .as_secs() as i64,
            }),
        }),
        options: None,
    });

    client.put(put_req).await.unwrap();
    wait_for_input();
    let get_req = Request::new(GetRequest {
        key: Some(BlobKey { key: 0 }),
        options: None,
    });

    client.get(get_req).await.unwrap();

    wait_for_input();
    let get_req2 = Request::new(GetRequest {
        key: Some(BlobKey { key: 1 }),
        options: None,
    });
    client.get(get_req2).await.unwrap();
}
