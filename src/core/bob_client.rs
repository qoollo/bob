use crate::api::grpc::{
    client::BobApi, Blob, BlobKey, BlobMeta, GetOptions, GetRequest, Null, PutOptions, PutRequest,
};
use crate::core::{
    backend::core::{BackendGetResult, BackendPingResult, BackendPutResult},
    backend::Error,
    data::{BobData, BobKey, BobMeta, ClusterResult, Node},
    metrics::*,
};
use tower_grpc::{BoxBody, Code, Request, Status};

use std::{pin::Pin, sync::Arc, time::Duration};
use tokio::{prelude::FutureExt, runtime::TaskExecutor};
use tower::MakeService;

use futures::Future;
use hyper::client::connect::{Destination, HttpConnector};
use tower_hyper::{client, util};

use futures03::{
    compat::Future01CompatExt, future::ready, future::FutureExt as OtherFutureExt,
    Future as NewFuture, TryFutureExt,
};
use futures_timer::ext::FutureExt as TimerExt;
use tower::buffer::Buffer;

type TowerConnect = Buffer<
    tower_request_modifier::RequestModifier<tower_hyper::Connection<BoxBody>, BoxBody>,
    http::request::Request<BoxBody>,
>;

#[derive(Clone)]
pub struct BobClient {
    node: Node,
    timeout: Duration,
    client: BobApi<TowerConnect>,
    metrics: BobClientMetrics,
}

pub type PutResult = Result<ClusterResult<BackendPutResult>, ClusterResult<Error>>;
pub struct Put(pub Pin<Box<dyn NewFuture<Output = PutResult> + Send>>);

pub type GetResult = Result<ClusterResult<BackendGetResult>, ClusterResult<Error>>;
pub struct Get(pub Pin<Box<dyn NewFuture<Output = GetResult> + Send>>);

pub type PingResult = Result<ClusterResult<BackendPingResult>, ClusterResult<Error>>;

impl BobClient {
    pub async fn new(
        node: Node,
        executor: TaskExecutor,
        timeout: Duration,
        metrics: BobClientMetrics,
    ) -> Result<Self, ()> {
        let dst = Destination::try_from_uri(node.get_uri()).unwrap();
        let connector = util::Connector::new(HttpConnector::new(4));
        let settings = client::Builder::new().http2_only(true).clone();
        let mut make_client = client::Connect::with_executor(connector, settings, executor.clone());
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

                BobApi::new(Buffer::with_executor(conn, 5, &mut executor.clone())).ready() //TODO add count treads
            })
            .map(move |client| BobClient {
                node,
                client,
                timeout,
                metrics,
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
            let mut client = self.client.clone();
            let timeout = self.timeout;

            let metrics = self.metrics.clone();
            let metrics2 = self.metrics.clone();
            metrics.put_count();
            let timer = metrics.put_timer();
            let timer2 = timer;

            let t = client.poll_ready();
            if let Err(err) = t {
                panic!("buffer inner error: {}", err);
            }
            if t.unwrap().is_not_ready() {
                debug!("service connection is not ready");
                let result = ClusterResult {
                    node: self.node.clone(),
                    result: Error::Failed("service connection is not ready".to_string()),
                };
                ready(Err(result)).boxed()
            } else {
                client
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
                    .map(move |_| {
                        metrics.put_timer_stop(timer);
                        ClusterResult {
                            node: n1,
                            result: BackendPutResult {},
                        }
                    })
                    .map_err(move |e| {
                        metrics2.put_error_count();
                        metrics2.put_timer_stop(timer2);

                        ClusterResult {
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
                        }
                    })
                    .compat()
                    .boxed()
            }
        })
    }

    pub fn get(&mut self, key: BobKey) -> Get {
        Get({
            let n1 = self.node.clone();
            let n2 = self.node.clone();
            let mut client = self.client.clone();
            let timeout = self.timeout;

            let metrics = self.metrics.clone();
            let metrics2 = self.metrics.clone();

            metrics.get_count();
            let timer = metrics.get_timer();
            let timer2 = timer;

            let t = client.poll_ready();
            if let Err(err) = t {
                panic!("buffer inner error: {}", err);
            }
            if t.unwrap().is_not_ready() {
                debug!("service connection is not ready");
                let result = ClusterResult {
                    node: self.node.clone(),
                    result: Error::Failed("service connection is not ready".to_string()),
                };
                ready(Err(result)).boxed()
            } else {
                client
                    .get(Request::new(GetRequest {
                        key: Some(BlobKey { key: key.key }),
                        options: Some(GetOptions { force_node: true }),
                    }))
                    .timeout(timeout)
                    .map(move |r| {
                        metrics.get_timer_stop(timer);
                        let ans = r.into_inner();
                        ClusterResult {
                            node: n1,
                            result: BackendGetResult {
                                data: BobData::new(ans.data, BobMeta::new(ans.meta.unwrap())),
                            },
                        }
                    })
                    .map_err(move |e| {
                        metrics2.get_error_count();
                        metrics2.get_timer_stop(timer2);
                        ClusterResult {
                            result: {
                                if e.is_elapsed() {
                                    Error::Timeout
                                } else if e.is_timer() {
                                    panic!("Timeout failed in core - can't continue")
                                } else {
                                    let err = e.into_inner();
                                    match err {
                                        Some(status) => match status.code() {
                                            tower_grpc::Code::NotFound => Error::KeyNotFound,
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
                        }
                    })
                    .compat()
                    .boxed()
            }
        })
    }

    pub async fn ping(&mut self) -> PingResult {
        let n1 = self.node.clone();
        let n2 = self.node.clone();
        let to = self.timeout;

        let mut client = self.client.clone();

        let t = client.poll_ready();
        if let Err(err) = t {
            panic!("buffer inner error: {}", err);
        }
        if t.unwrap().is_not_ready() {
            debug!("service connection is not ready");
            let result = ClusterResult {
                node: self.node.clone(),
                result: Error::Failed("service connection is not ready".to_string()),
            };
            Err(result)
        } else {
            client
                .ping(Request::new(Null {}))
                .map(move |_| ClusterResult {
                    node: n1,
                    result: BackendPingResult {},
                })
                .map_err(|e| Error::StorageError(format!("ping operation error: {}", e)))
                .compat()
                .boxed()
                .timeout(to)
                .map_err(move |e| ClusterResult {
                    node: n2.clone(),
                    result: e,
                })
                .await
        }
    }
}

#[derive(Clone)]
pub struct BobClientFactory {
    executor: TaskExecutor,
    timeout: Duration,
    metrics: Arc<dyn MetricsContainerBuilder + Send + Sync>,
}

impl BobClientFactory {
    pub fn new(
        executor: TaskExecutor,
        timeout: Duration,
        metrics: Arc<dyn MetricsContainerBuilder + Send + Sync>,
    ) -> Self {
        BobClientFactory {
            executor,
            timeout,
            metrics,
        }
    }
    pub(crate) async fn produce(&self, node: Node) -> Result<BobClient, ()> {
        let metrics = self.metrics.clone().get_metrics(&node.counter_display());
        BobClient::new(node, self.executor.clone(), self.timeout, metrics).await
    }
}
