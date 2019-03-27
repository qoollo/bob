use crate::core::data::{BobKey, BobData, Node, BobPutResult, BobErrorResult, BobPingResult};

use crate::api::grpc::{PutRequest,GetRequest, Null};

use crate::api::grpc::client::BobApi;
use tower_h2::client;
use tokio::net::tcp::{ConnectFuture, TcpStream};
use tower_grpc::Request;
use tower_service::Service;
use tower::MakeService;
use std::net::{SocketAddr};
use tokio::runtime::TaskExecutor;
use futures::{Future, Poll};

struct Dst {
    addr: SocketAddr
}
impl Dst {
    pub fn new(node: &Node) -> Dst {
        Dst {
            addr: SocketAddr::new(node.host.parse().unwrap(), node.port)
        }
    }
}
impl Service<()> for Dst {
    type Response = TcpStream;
    type Error = ::std::io::Error;
    type Future = ConnectFuture;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(().into())
    }

    fn call(&mut self, _: ()) -> Self::Future {
        TcpStream::connect(&self.addr)
    }
}

#[derive(Clone)]
pub struct BobClient {
    node: Node,
    //client: BobApi<T>
    client: BobApi<tower_add_origin::AddOrigin<tower_h2::client::Connection<tokio::net::TcpStream, tokio::runtime::TaskExecutor, tower_grpc::BoxBody>>>
}

impl BobClient {
    pub fn new(node: Node, executor: TaskExecutor) -> impl Future<Item=Self, Error=()> {
        println!("In new bobclient");
        let h2_settings = Default::default();
        let mut make_client = client::Connect::new(Dst::new(&node), h2_settings, executor);
        make_client.make_service(())
        .map(move |conn_l| {
            println!("COnnected to {:?}", node);
            let conn = tower_add_origin::Builder::new()
            .uri(node.get_uri())
            .build(conn_l)
            .unwrap();

            BobClient { 
                node: node,
                client: BobApi::new(conn)
            }
        })
        .map_err(|e| {
            println!("ERR = {:?}", e);
        })
    }

    pub fn put(&self, key: &BobKey, data: &BobData) -> impl Future<Item=BobPutResult, Error=BobErrorResult> {
        futures::future::ok(BobPutResult {})
    }

    pub fn ping(&mut self) -> impl Future<Item=BobPingResult, Error=BobErrorResult> {
        self.client.ping(Request::new(Null{})).map(|r| BobPingResult{}).map_err(|e| BobErrorResult{})
    }
}