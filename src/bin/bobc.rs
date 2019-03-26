
use futures::{Future, Poll};
use futures::stream::futures_unordered;
use futures::stream::futures_ordered;
use futures::stream::Stream;
use tokio::executor::DefaultExecutor;
use tokio::net::tcp::{ConnectFuture, TcpStream};
use tower_grpc::Request;
use tower_h2::client;
use tower_service::Service;
use tower::MakeService;
use std::io;

use bob::api::grpc::client::BobApi;
use bob::api::grpc::{PutRequest,GetRequest};

use tokio::runtime::{Runtime};

fn main() {

    let mut rt = Runtime::new().unwrap();
    let uri: http::Uri = format!("http://localhost:20000").parse().unwrap();

    let h2_settings = Default::default();
    let mut make_client = client::Connect::new(Dst, h2_settings, rt.executor());

    let conn_l = rt.block_on(make_client.make_service(())).unwrap();
    let conn = tower_add_origin::Builder::new()
            .uri(uri)
            .build(conn_l)
            .unwrap();
    let mut client = BobApi::new(conn);


    let pur_req:Vec<_> = (0..10).map(|_| client.put(Request::new(
        PutRequest { key: None, data: None, options: None}))
    /*            .map_err(|e| println!("gRPC request failed; err={:?}", e))*/
    .and_then(|response| {
        println!("RESPONSE = {:?}", response);
        Ok(())
    })
    /*.map_err(|e| {
        println!("ERR = {:?}", e);
    })*/).collect();

    let rqs = futures_unordered(pur_req).collect();
    rt.block_on(rqs);

    println!("Press any key to send GET");

    io::stdin().read_line(&mut String::default());
    let get_req = client.get(Request::new(GetRequest { key: None, options: None}))
    .map_err(|e| println!("gRPC request failed; err={:?}", e))
    .and_then(|response| {
        println!("RESPONSE = {:?}", response);
        Ok(())
    })
    .map_err(|e| {
        println!("ERR = {:?}", e);
    });    

    rt.block_on(get_req);

    println!("Press any key to send second GET");

    io::stdin().read_line(&mut String::default());
    let get_req2 = client.get(Request::new(GetRequest { key: None, options: None}))
    .map_err(|e| println!("gRPC request failed; err={:?}", e))
    .and_then(|response| {
        println!("RESPONSE = {:?}", response);
        Ok(())
    })
    .map_err(|e| {
        println!("ERR = {:?}", e);
    });    

    rt.block_on(get_req2);
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
