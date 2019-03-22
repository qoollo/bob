
use futures::{Future, Poll};
use tokio::executor::DefaultExecutor;
use tokio::net::tcp::{ConnectFuture, TcpStream};
use tower_grpc::Request;
use tower_h2::client;
use tower_service::Service;
use tower::MakeService;


fn main() {
    let uri: http::Uri = format!("http://localhost:20000").parse().unwrap();

    let h2_settings = Default::default();
    let mut make_client = client::Connect::new(Dst, h2_settings, DefaultExecutor::current());

    let pur_req = make_client.make_service(())
    .map(move |conn| {
        use bob::api::grpc::client::BobApi;

        let conn = tower_add_origin::Builder::new()
            .uri(uri)
            .build(conn)
            .unwrap();

        BobApi::new(conn)
    })
    .and_then(|mut client| {
        use bob::api::grpc::PutRequest;

        client.put(Request::new(PutRequest { key: None, data: None, options: None}))
                .map_err(|e| panic!("gRPC request failed; err={:?}", e))
    })
    .and_then(|response| {
        println!("RESPONSE = {:?}", response);
        Ok(())
    })
    .map_err(|e| {
        println!("ERR = {:?}", e);
    });

    tokio::run(pur_req);

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
