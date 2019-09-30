mod b_client {
    use super::{Get, PingResult, Put};
    use crate::api::grpc::{
        client::BobApi, Blob, BlobKey, BlobMeta, GetOptions, GetRequest, Null, PutOptions,
        PutRequest,
    };
    use crate::core::{
        backend::core::{BackendGetResult, BackendPingResult, BackendPutResult},
        backend::Error,
        data::{BobData, BobKey, BobMeta, ClusterResult, Node},
        metrics::*,
    };
    use tower_grpc::{BoxBody, Code, Request, Status};

    use std::time::Duration;
    use tokio::{prelude::FutureExt, runtime::TaskExecutor};
    use tower::MakeService;

    use futures::Future;
    use hyper::client::connect::{Destination, HttpConnector};
    use tower_hyper::{client, util};

    use futures03::{
        compat::Future01CompatExt, future::ready, future::FutureExt as OtherFutureExt,
    };
    use mockall::*;
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

    impl BobClient {
        pub async fn create(
            node: Node,
            executor: TaskExecutor,
            timeout: Duration,
            buffer_bound: u16,
            metrics: BobClientMetrics,
        ) -> Result<Self, ()> {
            let dst = Destination::try_from_uri(node.get_uri()).unwrap();
            let mut http_connector = HttpConnector::new(4);
            http_connector.set_nodelay(true);

            let connector = util::Connector::new(http_connector);
            let settings = client::Builder::new().http2_only(true).clone();
            let mut make_client =
                client::Connect::with_executor(connector, settings, executor.clone());
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

                    BobApi::new(Buffer::with_executor(
                        conn,
                        buffer_bound as usize,
                        &mut executor.clone(),
                    ))
                    .ready() //TODO add count treads
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

        pub fn put(&mut self, key: BobKey, d: &BobData, options: PutOptions) -> Put {
            Put({
                let n1 = self.node.clone();
                let n2 = self.node.clone();
                let mut client = self.client.clone();
                let timeout = self.timeout;

                let request = Request::new(PutRequest {
                    key: Some(BlobKey { key: key.key }),
                    data: Some(Blob {
                        data: d.data.clone(),
                        meta: Some(BlobMeta {
                            timestamp: d.meta.timestamp,
                        }),
                    }),
                    options: Some(options),
                });

                let metrics = self.metrics.clone();
                let metrics2 = self.metrics.clone();
                metrics.put_count();

                let timer = metrics.put_timer();

                let t = client.poll_ready();
                if let Err(err) = t {
                    debug!("buffer inner error: {}", err);
                    let result = ClusterResult {
                        node: self.node.clone(),
                        result: Error::Failed(format!("buffer inner error: {}", err)),
                    };
                    ready(Err(result)).boxed()
                } else if t.unwrap().is_not_ready() {
                    debug!("service connection is not ready");
                    let result = ClusterResult {
                        node: self.node.clone(),
                        result: Error::Failed("service connection is not ready".to_string()),
                    };
                    ready(Err(result)).boxed()
                } else {
                    client
                        .put(request)
                        // .timeout(timeout)   //TODO
                        .map(move |_| {
                            metrics.put_timer_stop(timer);
                            ClusterResult {
                                node: n1,
                                result: BackendPutResult {},
                            }
                        })
                        .map_err(move |e| {
                            metrics2.put_error_count();
                            metrics2.put_timer_stop(timer);

                            ClusterResult {
                                result: Error::from(e),
                                node: n2,
                            }
                        })
                        .compat()
                        .boxed()
                }
            })
        }

        pub fn get(&mut self, key: BobKey, options: GetOptions) -> Get {
            Get({
                let n1 = self.node.clone();
                let n2 = self.node.clone();
                let mut client = self.client.clone();
                let timeout = self.timeout;

                let metrics = self.metrics.clone();
                let metrics2 = self.metrics.clone();

                metrics.get_count();
                let timer = metrics.get_timer();

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
                            options: Some(options),
                        }))
                        // .timeout(timeout)   //TODO
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
                            metrics2.get_timer_stop(timer);
                            ClusterResult {
                                result: Error::from(e),
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
                    // .timeout(to) //TODO
                    .map(move |_| ClusterResult {
                        node: n1,
                        result: BackendPingResult {},
                    })
                    .map_err(move |e| ClusterResult {
                        node: n2.clone(),
                        result: Error::from(e),
                    })
                    .compat()
                    .boxed()
                    .await
            }
        }
    }

    mock! {
        pub BobClient {
            async fn create(node: Node, executor: TaskExecutor, timeout: Duration, buffer_bound: u16, metrics: BobClientMetrics,
                    ) -> Result<Self, ()>;
            fn put(&mut self, key: BobKey, d: &BobData, options: PutOptions) -> Put;
            fn get(&mut self, key: BobKey, options: GetOptions) -> Get;
            async fn ping(&mut self) -> PingResult;
        }
        trait Clone {
            fn clone(&self) -> Self;
        }
    }
}

cfg_if! {
    if #[cfg(test)] {
        pub use self::b_client::MockBobClient as BobClient;
    } else {
        pub use self::b_client::BobClient;
    }
}

use crate::core::{
    backend::core::{BackendGetResult, BackendPingResult, BackendPutResult},
    backend::Error,
    data::{ClusterResult, Node},
    metrics::*,
};
use futures03::Future as Future03;
use std::{pin::Pin, sync::Arc, time::Duration};
use tokio::runtime::TaskExecutor;

pub type PutResult = Result<ClusterResult<BackendPutResult>, ClusterResult<Error>>;
pub struct Put(pub Pin<Box<dyn Future03<Output = PutResult> + Send>>);

pub type GetResult = Result<ClusterResult<BackendGetResult>, ClusterResult<Error>>;
pub struct Get(pub Pin<Box<dyn Future03<Output = GetResult> + Send>>);

pub type PingResult = Result<ClusterResult<BackendPingResult>, ClusterResult<Error>>;

#[derive(Clone)]
pub struct BobClientFactory {
    executor: TaskExecutor,
    timeout: Duration,
    buffer_bound: u16,
    metrics: Arc<dyn MetricsContainerBuilder + Send + Sync>,
}

impl BobClientFactory {
    pub fn new(
        executor: TaskExecutor,
        timeout: Duration,
        buffer_bound: u16,
        metrics: Arc<dyn MetricsContainerBuilder + Send + Sync>,
    ) -> Self {
        BobClientFactory {
            executor,
            timeout,
            buffer_bound,
            metrics,
        }
    }
    pub(crate) async fn produce(&self, node: Node) -> Result<BobClient, ()> {
        let metrics = self.metrics.clone().get_metrics(&node.counter_display());
        BobClient::create(
            node,
            self.executor.clone(),
            self.timeout,
            self.buffer_bound,
            metrics,
        )
        .await
    }
}

pub mod tests {
    use super::*;
    use crate::core::{
        backend::core::{BackendPingResult, BackendPutResult},
        backend::Error,
        data::{BobData, BobMeta, ClusterResult, Node},
    };
    use futures03::{future::ready, future::FutureExt as OtherFutureExt};

    pub fn ping_ok(node: Node) -> PingResult {
        Ok(ClusterResult {
            node,
            result: BackendPingResult {},
        })
    }
    pub fn ping_err(node: Node) -> PingResult {
        Err(ClusterResult {
            node,
            result: Error::Internal,
        })
    }

    pub fn put_ok(node: Node) -> Put {
        Put({
            ready(Ok(ClusterResult {
                node,
                result: BackendPutResult {},
            }))
            .boxed()
        })
    }

    pub fn put_err(node: Node) -> Put {
        Put({
            ready(Err(ClusterResult {
                node,
                result: Error::Internal,
            }))
            .boxed()
        })
    }

    pub fn get_ok(node: Node, timestamp: i64) -> Get {
        Get({
            ready(Ok(ClusterResult {
                node,
                result: BackendGetResult {
                    data: BobData::new(vec![], BobMeta::new_value(timestamp)),
                },
            }))
            .boxed()
        })
    }

    pub fn get_err(node: Node) -> Get {
        Get({
            ready(Err(ClusterResult {
                node,
                result: Error::Internal,
            }))
            .boxed()
        })
    }
}
