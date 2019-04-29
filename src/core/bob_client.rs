use crate::core::data::{
    BobData, BobError, BobGetResult, BobKey, BobPingResult, BobPutResult, ClusterResult, Node,
};
use tower_grpc::BoxBody;
use tower_h2::client::Connection;

use crate::api::grpc::{Blob, BlobKey, GetOptions, GetRequest, Null, PutOptions, PutRequest};

use crate::api::grpc::client::BobApi;
use futures::{Future, Poll};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::tcp::TcpStream;
use tokio::prelude::FutureExt;
use tokio::runtime::TaskExecutor;
use tower::MakeService;
use tower_grpc::Request;
use tower_h2::client;
use tower_service::Service;

struct Dst {
    addr: SocketAddr,
}
impl Dst {
    pub fn new(node: &Node) -> Dst {
        Dst {
            addr: SocketAddr::new(node.host.parse().unwrap(), node.port),
        }
    }
}
impl Service<()> for Dst {
    type Response = TcpStream;
    type Error = std::io::Error;
    type Future = Box<Future<Item = TcpStream, Error = ::std::io::Error> + Send>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        Ok(().into())
    }

    fn call(&mut self, _: ()) -> Self::Future {
        Box::new(
            TcpStream::connect(&self.addr)
                .map(|stream| {
                    stream.set_nodelay(true).unwrap();
                    stream
                })
                .map_err(|err| err),
        )
    }
}

#[derive(Clone)]
pub struct BobClient {
    node: Node,
    timeout: Duration,
    client: BobApi<
        tower_request_modifier::RequestModifier<
            Connection<TcpStream, TaskExecutor, BoxBody>,
            BoxBody,
        >,
    >,
}

impl BobClient {
    pub fn new(
        node: Node,
        executor: TaskExecutor,
        timeout: Duration,
    ) -> impl Future<Item = Self, Error = ()> {
        let h2_settings = Default::default();
        let mut make_client = client::Connect::new(Dst::new(&node), h2_settings, executor);
        make_client
            .make_service(())
            .map(move |conn_l| {
                trace!("COnnected to {:?}", node);
                let conn = tower_request_modifier::Builder::new()
                    .set_origin(node.get_uri())
                    .build(conn_l)
                    .unwrap();

                BobClient {
                    node,
                    client: BobApi::new(conn),
                    timeout,
                }
            })
            .map_err(|e| {
                debug!("BobClient: ERR = {:?}", e);
            })
    }

    pub fn put(
        &mut self,
        key: BobKey,
        data: &BobData,
    ) -> impl Future<Item = ClusterResult<BobPutResult>, Error = ClusterResult<BobError>> {
        let n1 = self.node.clone();
        let n2 = self.node.clone();
        self.client
            .put(Request::new(PutRequest {
                key: Some(BlobKey { key: key.key }),
                data: Some(Blob {
                    data: data.data.clone(), // TODO: find way to eliminate data copying
                }),
                options: Some(PutOptions {
                    force_node: true,
                    overwrite: false,
                }),
            }))
            .timeout(self.timeout)
            .map(|_| ClusterResult {
                node: n1,
                result: BobPutResult {},
            })
            .map_err(move |e| ClusterResult {
                result: {
                    if e.is_elapsed() {
                        BobError::Timeout
                    } else if e.is_timer() {
                        panic!("Timeout failed in core - can't continue")
                    } else {
                        let err = e.into_inner();
                        BobError::Other(format!("Put operation for {} failed: {:?}", n2, err))
                    }
                },
                node: n2,
            })
    }

    pub fn get(
        &mut self,
        key: BobKey,
    ) -> impl Future<Item = ClusterResult<BobGetResult>, Error = ClusterResult<BobError>> {
        let n1 = self.node.clone();
        let n2 = self.node.clone();
        self.client
            .get(Request::new(GetRequest {
                key: Some(BlobKey { key: key.key }),
                options: Some(GetOptions { force_node: true }),
            }))
            .timeout(self.timeout)
            .map(|r| {
                let ans = r.into_inner();
                ClusterResult {
                    node: n1,
                    result: BobGetResult {
                        data: BobData { data: ans.data },
                    },
                }
            })
            .map_err(move |e| ClusterResult {
                result: {
                    if e.is_elapsed() {
                        BobError::Timeout
                    } else if e.is_timer() {
                        panic!("Timeout failed in core - can't continue")
                    } else {
                        let err = e.into_inner();
                        match err {
                            Some(status) => match status.code() {
                                tower_grpc::Code::NotFound => BobError::NotFound,
                                _ => BobError::Other(format!(
                                    "Get operation for {} failed: {:?}",
                                    n2, status
                                )),
                            },
                            None => BobError::Other(format!(
                                "Get operation for {} failed: {:?}",
                                n2, err
                            )),
                        }
                    }
                },
                node: n2,
            })
    }

    pub fn ping(&mut self) -> impl Future<Item = BobPingResult, Error = BobError> {
        let n1 = self.node.clone();
        let n2 = self.node.clone();
        self.client
            .ping(Request::new(Null {}))
            .timeout(self.timeout)
            .map(move |_| BobPingResult { node: n1 })
            .map_err(move |e| {
                if e.is_elapsed() {
                    BobError::Timeout
                } else if e.is_timer() {
                    panic!("Timeout can't failed in core - can't continue")
                } else {
                    let err = e.into_inner();
                    BobError::Other(format!("Ping operation for {} failed: {:?}", n2, err))
                }
            })
    }
}

#[derive(Clone)]
pub struct BobClientFactory {
    pub executor: TaskExecutor,
    pub timeout: Duration,
}

impl BobClientFactory {
    pub fn produce(&self, node: Node) -> impl Future<Item = BobClient, Error = ()> {
        BobClient::new(node, self.executor.clone(), self.timeout)
    }
}
