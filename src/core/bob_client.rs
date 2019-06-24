use crate::api::grpc::{
    client::BobApi, Blob, BlobKey, BlobMeta, GetOptions, GetRequest, Null, PutOptions, PutRequest,
};
use crate::core::{
    backend::backend::{BackendGetResult, BackendPingResult, BackendPutResult, Error},
    data::{BobData, BobKey, BobMeta, ClusterResult, Node},
};
use tower_grpc::{BoxBody, Code, Request, Status};

use std::{pin::Pin, sync::Arc, time::Duration};
use tokio::{prelude::FutureExt, runtime::TaskExecutor};
use tower::MakeService;

use futures::Future;
use hyper::client::connect::{Destination, HttpConnector};
use tower_hyper::{client, util};

use futures_locks::Mutex;

use futures03::{
    compat::Future01CompatExt, future::FutureExt as OtherFutureExt, Future as NewFuture,
};

type TowerConnect =
    tower_request_modifier::RequestModifier<tower_hyper::Connection<BoxBody>, BoxBody>;
#[derive(Clone)]
pub struct BobClient {
    node: Node,
    timeout: Duration,
    client: Arc<Mutex<BobApi<TowerConnect>>>,
}

pub type PutResult = Result<ClusterResult<BackendPutResult>, ClusterResult<Error>>;
pub struct Put(pub Pin<Box<dyn NewFuture<Output = PutResult> + Send>>);

pub type GetResult = Result<ClusterResult<BackendGetResult>, ClusterResult<Error>>;
pub struct Get(pub Pin<Box<dyn NewFuture<Output = GetResult> + Send>>);

pub type PingResult = Result<ClusterResult<BackendPingResult>, ClusterResult<Error>>;

impl BobClient {
    pub async fn new(node: Node, executor: TaskExecutor, timeout: Duration) -> Result<Self, ()> {
        let dst = Destination::try_from_uri(node.get_uri()).unwrap();
        let connector = util::Connector::new(HttpConnector::new(4));
        let settings = client::Builder::new().http2_only(true).clone();
        let mut make_client = client::Connect::with_executor(connector, settings, executor);
        let n1 = node.clone();
        make_client
            .make_service(dst)
            .map_err(|e| Status::new(Code::Unavailable, format!("Connection error: {}", e)))
            .and_then(move |conn_l| {
                trace!("Connected to {:?}", n1);
                let conn = tower_request_modifier::Builder::new()
                    .set_origin(n1.get_uri())
                    .build(conn_l)
                    .unwrap();
                BobApi::new(conn).ready()
            })
            .map(move |client| BobClient {
                node,
                client: Arc::new(Mutex::new(client)),
                timeout,
            })
            .map_err(|e| debug!("BobClient: ERR = {:?}", e))
            .compat()
            .boxed()
            .await
    }

    pub fn put(&mut self, key: BobKey, d: &BobData) -> Put {
        Put({
            let n1 = self.node.clone();
            let n2 = self.node.clone();
            let data = d.clone(); // TODO: find way to eliminate data copying
            let client = self.client.clone();
            let timeout = self.timeout;

            client
                .lock()
                .then(move |client_res| {
                    match client_res {
                        Ok(mut cl) => cl
                            .put(Request::new(PutRequest {
                                key: Some(BlobKey { key: key.key }),
                                data: Some(Blob {
                                    data: data.data,
                                    meta: Some(BlobMeta {
                                        timestamp: data.meta.timestamp,
                                    }),
                                }),
                                options: Some(PutOptions {
                                    force_node: true,
                                    overwrite: false,
                                }),
                            }))
                            .timeout(timeout)
                            .map(|_| ClusterResult {
                                node: n1,
                                result: BackendPutResult {},
                            })
                            .map_err(move |e| ClusterResult {
                                result: {
                                    if e.is_elapsed() {
                                        Error::Timeout
                                    } else if e.is_timer() {
                                        panic!("Timeout failed in core - can't continue")
                                    } else {
                                        let err = e.into_inner();
                                        Error::Failed(format!(
                                            "Put operation for {} failed: {:?}",
                                            n2, err
                                        ))
                                    }
                                },
                                node: n2,
                            }),
                        Err(_) => panic!("Timeout failed in core - can't continue"), //TODO
                    }
                })
                .compat()
                .boxed()
        })
    }

    pub fn get(&mut self, key: BobKey) -> Get {
        Get({
            let n1 = self.node.clone();
            let n2 = self.node.clone();
            let client = self.client.clone();
            let timeout = self.timeout;

            client
                .lock()
                .then(move |client_res| {
                    match client_res {
                        Ok(mut cl) => cl
                            .get(Request::new(GetRequest {
                                key: Some(BlobKey { key: key.key }),
                                options: Some(GetOptions { force_node: true }),
                            }))
                            .timeout(timeout)
                            .map(|r| {
                                let ans = r.into_inner();
                                ClusterResult {
                                    node: n1,
                                    result: BackendGetResult {
                                        data: BobData::new(
                                            ans.data,
                                            BobMeta::new(ans.meta.unwrap()),
                                        ),
                                    },
                                }
                            })
                            .map_err(move |e| ClusterResult {
                                result: {
                                    if e.is_elapsed() {
                                        Error::Timeout
                                    } else if e.is_timer() {
                                        panic!("Timeout failed in core - can't continue")
                                    } else {
                                        let err = e.into_inner();
                                        match err {
                                            Some(status) => match status.code() {
                                                tower_grpc::Code::NotFound => Error::NotFound,
                                                _ => Error::Failed(format!(
                                                    "Get operation for {} failed: {:?}",
                                                    n2, status
                                                )),
                                            },
                                            None => Error::Failed(format!(
                                                "Get operation for {} failed: {:?}",
                                                n2, err
                                            )),
                                        }
                                    }
                                },
                                node: n2,
                            }),
                        Err(_) => panic!("Timeout failed in core - can't continue"), //TODO
                    }
                })
                .compat()
                .boxed()
        })
    }

    pub async fn ping(&mut self) -> PingResult {
        let n1 = self.node.clone();
        let n2 = self.node.clone();
        let to = self.timeout;
        self.client
            .lock()
            .then(move |client_res| match client_res {
                Ok(mut cl) => cl
                    .ping(Request::new(Null {}))
                    .timeout(to)
                    .map(move |_| ClusterResult {
                        node: n1,
                        result: BackendPingResult {},
                    })
                    .map_err(move |e| ClusterResult {
                        node: n2.clone(),
                        result: {
                            if e.is_elapsed() {
                                Error::Timeout
                            } else if e.is_timer() {
                                panic!("Timeout can't failed in core - can't continue")
                            } else {
                                let err = e.into_inner();
                                Error::Failed(format!(
                                    "Ping operation for {} failed: {:?}",
                                    n2, err
                                ))
                            }
                        },
                    }),
                Err(_) => panic!("Timeout failed in core - can't continue"), //TODO
            })
            .compat()
            .boxed()
            .await
    }
}

#[derive(Clone)]
pub struct BobClientFactory {
    pub executor: TaskExecutor,
    pub timeout: Duration,
}

impl BobClientFactory {
    pub async fn produce(&self, node: Node) -> Result<BobClient, ()> {
        BobClient::new(node, self.executor.clone(), self.timeout).await
    }
}
