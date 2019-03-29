use crate::core::data::{BobKey, BobData, Node, BobPutResult, BobError, BobPingResult};

use crate::api::grpc::{PutRequest,GetRequest, Null, BlobKey, Blob, PutOptions};

use crate::api::grpc::client::BobApi;
use tower_h2::client;
use tokio::net::tcp::{ConnectFuture, TcpStream};
use tower_grpc::Request;
use tower_service::Service;
use tower::MakeService;
use std::net::{SocketAddr};
use tokio::runtime::TaskExecutor;
use futures::{Future, Poll};
use tokio::prelude::FutureExt;
use std::time::Duration;

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
    timeout: Duration,
    client: BobApi<tower_add_origin::AddOrigin<tower_h2::client::Connection<tokio::net::TcpStream, tokio::runtime::TaskExecutor, tower_grpc::BoxBody>>>
}

impl BobClient {
    pub fn new(node: Node, executor: TaskExecutor, timeout: Duration) -> impl Future<Item=Self, Error=()> {
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
                node,
                client: BobApi::new(conn),
                timeout
            }
        })
        .map_err(|e| {
            println!("ERR = {:?}", e);
        })
    }

    pub fn put(&mut self, key: BobKey, data: &BobData) -> impl Future<Item=BobPutResult, Error=BobError> {
        self.client.put(Request::new(PutRequest{ 
                key: Some(BlobKey{
                    key: key.key
                }), data: Some(Blob{
                    data: data.data.clone() // TODO: find way to eliminate data copying
                }), options: Some(PutOptions{
                    force_node: true,
                    overwrite: false
                })
            }))
            .timeout(self.timeout)
            .map(|_| BobPutResult{})
            .map_err(|e| {
                if e.is_elapsed() {
                    BobError::Timeout
                } else if e.is_timer() {
                    panic!("Timeout can't failed in core - can't continue")
                } else {
                    let err = e.into_inner();
                    BobError::Other(format!("Put operation for failed: {:?}", err))
                }
            })
    }

    pub fn ping(&mut self) -> impl Future<Item=BobPingResult, Error=BobError> {
        self.client.ping(Request::new(Null{}))
            .timeout(self.timeout)
            .map(|_| BobPingResult{})
            .map_err(|e|  {
                if e.is_elapsed() {
                    BobError::Timeout
                } else if e.is_timer() {
                    panic!("Timeout can't failed in core - can't continue")
                } else {
                    let err = e.into_inner();
                    BobError::Other(format!("Ping operation failed: {:?}", err))
                }
            })
    }
}