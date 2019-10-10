mod b_client {
    use super::super::prelude::*;
    use super::*;

    use super::PingResult;
    use hyper::client::connect::{Destination, HttpConnector};
    use mockall::*;
    use tokio::future::FutureExt as TokioFutureExt;

    // type TowerConnect = Buffer<
    //     tower_request_modifier::RequestModifier<tower_hyper::Connection<BoxBody>, BoxBody>,
    //     http::request::Request<BoxBody>,
    // >;

    #[derive(Clone)]
    pub struct BobClient {
        node: Node,
        timeout: Duration,
        client: BobApiClient<tonic::transport::Channel>,
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

            BobApiClient::connect(node.get_uri())
                .map(|client| BobClient {
                    node,
                    client,
                    timeout,
                    metrics,
                })
                .map_err(|e| error!("{}", e.to_string()))
        }

        pub fn put(&mut self, key: BobKey, d: &BobData, options: PutOptions) -> Put {
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
            self.metrics.put_count();
            let timer = self.metrics.put_timer();

            let mut client = self.client.clone();
            let metrics = self.metrics.clone();
            let node = self.node.clone();
            let timeout = self.timeout;
            let res = async move {
                client
                    .put(request)
                    .map(|res| {
                        metrics.put_timer_stop(timer);
                        ClusterResult {
                            node: node.clone(),
                            result: BackendPutResult {},
                        }
                    })
                    .timeout(timeout)
                    .await
                    .map_err(|e| {
                        metrics.put_error_count();
                        metrics.put_timer_stop(timer);
                        ClusterResult {
                            result: BackendError::from(e),
                            node,
                        }
                    })
            };
            Put(res.boxed())
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

                // let t = client.poll_ready();
                // if let Err(err) = t {
                //     panic!("buffer inner error: {}", err);
                // }
                // if t.unwrap().is_not_ready() {
                //     debug!("service connection is not ready");
                //     let result = ClusterResult {
                //         node: self.node.clone(),
                //         result: BackendError::Failed("service connection is not ready".to_string()),
                //     };
                //     ready(Err(result)).boxed()
                // } else {
                //     client
                //         .get(Request::new(GetRequest {
                //             key: Some(BlobKey { key: key.key }),
                //             options: Some(options),
                //         }))
                //         .map(move |r| {
                //             metrics.get_timer_stop(timer);
                //             let ans = r.into_inner();
                //             ClusterResult {
                //                 node: n1,
                //                 result: BackendGetResult {
                //                     data: BobData::new(ans.data, BobMeta::new(ans.meta.unwrap())),
                //                 },
                //             }
                //         })
                //         .map_err(move |e| BackendError::from(e))
                //         .compat()
                //         .boxed()
                //         .timeout(timeout)
                //         .map(move |r| {
                //             if r.is_err() {
                //                 metrics2.get_error_count();
                //                 metrics2.get_timer_stop(timer);
                //             }
                //             r.map_err(|e| ClusterResult {
                //                 result: e,
                //                 node: n2,
                //             })
                //         })
                //         .boxed()
                // }
                unimplemented!()
            })
        }

        pub async fn ping(&mut self) -> PingResult {
            let mut client = self.client.clone();
            let ping_res = client
                .ping(Request::new(Null {}))
                .map(|res| ClusterResult {
                    node: self.node.clone(),
                    result: BackendPingResult {},
                })
                .timeout(self.timeout)
                .await;
            ping_res.map_err(|e| ClusterResult {
                node: self.node.clone(),
                result: BackendError::from(e),
            })
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

use super::prelude::*;

pub type PutResult = Result<ClusterResult<BackendPutResult>, ClusterResult<BackendError>>;
pub struct Put(pub Pin<Box<dyn Future<Output = PutResult> + Send>>);

pub type GetResult = Result<ClusterResult<BackendGetResult>, ClusterResult<BackendError>>;
pub struct Get(pub Pin<Box<dyn Future<Output = GetResult> + Send>>);

pub type PingResult = Result<ClusterResult<BackendPingResult>, ClusterResult<BackendError>>;

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
    use crate::core_inner::{
        backend::Error,
        backend::{BackendPingResult, BackendPutResult},
        data::{BobData, BobMeta, ClusterResult, Node},
    };
    use futures::{future::ready, future::FutureExt as OtherFutureExt};

    pub fn ping_ok(node: Node) -> PingResult {
        Ok(ClusterResult {
            node,
            result: BackendPingResult {},
        })
    }
    pub fn ping_err(node: Node) -> PingResult {
        Err(ClusterResult {
            node,
            result: BackendError::Internal,
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
                result: BackendError::Internal,
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
                result: BackendError::Internal,
            }))
            .boxed()
        })
    }
}
