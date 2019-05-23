use crate::api::grpc::{
    Blob, BlobKey, BlobMeta, GetOptions, GetRequest, Null, PutOptions, PutRequest,
};
use crate::core::data::{
    BobData, BobError, BobGetResult, BobKey, BobMeta, BobPingResult, BobPutResult, ClusterResult,
    Node,
};
use tower_grpc::BoxBody;

use crate::api::grpc::client::BobApi;
use std::time::Duration;
use tokio::prelude::FutureExt;
use tokio::runtime::TaskExecutor;
use tower::MakeService;
use tower_grpc::Request;

use futures::Future;
use hyper::client::connect::{Destination, HttpConnector};
use tower_hyper::{client, util};

use futures_locks::Mutex;
use std::sync::Arc;
use tower_grpc::{Code, Status};

type TowerConnect =
    tower_request_modifier::RequestModifier<tower_hyper::Connection<BoxBody>, BoxBody>;
#[derive(Clone)]
pub struct BobClient {
    node: Node,
    timeout: Duration,
    client: Arc<Mutex<BobApi<TowerConnect>>>,
}

pub struct Put(
    pub Box<dyn Future<Item = ClusterResult<BobPutResult>, Error = ClusterResult<BobError>> + Send>,
);
pub struct Get(
    pub Box<dyn Future<Item = ClusterResult<BobGetResult>, Error = ClusterResult<BobError>> + Send>,
);

impl BobClient {
    pub fn new(
        node: Node,
        executor: TaskExecutor,
        timeout: Duration,
    ) -> impl Future<Item = Self, Error = ()> {
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
    }

    pub fn put(&mut self, key: BobKey, d: &BobData) -> Put {
        Put({
            let n1 = self.node.clone();
            let n2 = self.node.clone();
            let data = d.clone();
            let client = self.client.clone();
            let timeout = self.timeout;

            Box::new(client.lock().then(move |client_res| match client_res {
                Ok(mut cl) => {
                    cl.put(Request::new(PutRequest {
                        key: Some(BlobKey { key: key.key }),
                        data: Some(Blob {
                            data: data.data.clone(), // TODO: find way to eliminate data copying
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
                                BobError::Other(format!(
                                    "Put operation for {} failed: {:?}",
                                    n2, err
                                ))
                            }
                        },
                        node: n2,
                    })
                }
                Err(_) => panic!("Timeout failed in core - can't continue"), //TODO
            }))
        })
    }

    pub fn get(&mut self, key: BobKey) -> Get {
        Get({
            let n1 = self.node.clone();
            let n2 = self.node.clone();
            let client = self.client.clone();
            let timeout = self.timeout;

            Box::new(client.lock().then(move |client_res| {
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
                                result: BobGetResult {
                                    data: BobData {
                                        data: ans.data,
                                        meta: BobMeta::new(ans.meta.unwrap()),
                                    },
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
                        }),
                    Err(_) => panic!("Timeout failed in core - can't continue"), //TODO
                }
            }))
        })
    }

    pub fn ping(&mut self) -> impl Future<Item = BobPingResult, Error = BobError> {
        let n1 = self.node.clone();
        let n2 = self.node.clone();
        let to = self.timeout;
        self.client.lock().then(move |client_res| match client_res {
            Ok(mut cl) => cl
                .ping(Request::new(Null {}))
                .timeout(to)
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
                }),
            Err(_) => panic!("Timeout failed in core - can't continue"), //TODO
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
