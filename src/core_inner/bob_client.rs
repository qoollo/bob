pub(crate) mod b_client {
    use super::super::prelude::*;
    use super::*;
    use crate::grpc::{Blob, BlobMeta};
    use crate::grpc::{GetOptions, GetRequest, PutOptions, PutRequest};

    use std::time::Duration;
    use tonic::Request;

    use super::PingResult;
    use mockall::*;

    #[derive(Clone)]
    pub struct RealBobClient {
        node: Node,
        operation_timeout: Duration,
        client: BobApiClient<Channel>,
        metrics: BobClientMetrics,
    }

    impl RealBobClient {
        #[allow(dead_code)]
        pub async fn create(
            node: Node,
            operation_timeout: Duration,
            metrics: BobClientMetrics,
        ) -> Result<Self, String> {
            let endpoint = Endpoint::from(node.get_uri()).tcp_nodelay(true);
            let client = BobApiClient::connect(endpoint)
                .await
                .map_err(|e| e.to_string())?;
            Ok(Self {
                node,
                client,
                operation_timeout,
                metrics,
            })
        }

        // Getters
        #[allow(dead_code)]
        pub fn node(&self) -> &Node {
            &self.node
        }

        #[allow(dead_code)]
        pub fn put(&mut self, key: BobKey, d: &BobData, options: PutOptions) -> Put {
            debug!("real client put called");
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
            let t = self.operation_timeout;
            let res = async move {
                timeout(
                    t,
                    client.put(request).map(|res| {
                        res.expect("client put request");
                        metrics.put_timer_stop(timer);
                        ClusterResult {
                            node: node.clone(),
                            result: BackendPutResult {},
                        }
                    }),
                )
                .await
                .map_err(|_| {
                    metrics.put_error_count();
                    metrics.put_timer_stop(timer);
                    ClusterResult {
                        result: BackendError::Timeout,
                        node,
                    }
                })
            };
            Put(res.boxed())
        }

        #[allow(dead_code)]
        pub fn get(&mut self, key: BobKey, options: GetOptions) -> Get {
            let n1 = self.node.clone();
            let n2 = self.node.clone();
            let mut client = self.client.clone();
            let t = self.operation_timeout;

            let metrics = self.metrics.clone();
            let metrics2 = self.metrics.clone();

            metrics.get_count();
            let timer = metrics.get_timer();

            let task = async move {
                let res = timeout(
                    t,
                    client.get(Request::new(GetRequest {
                        key: Some(BlobKey { key: key.key }),
                        options: Some(options),
                    })),
                )
                .await
                .expect("client get with timeout");
                res.map(move |r| {
                    metrics.get_timer_stop(timer);
                    let ans = r.into_inner();
                    ClusterResult {
                        node: n1,
                        result: BackendGetResult {
                            data: BobData::new(
                                ans.data,
                                BobMeta::new(ans.meta.expect("get blob meta")),
                            ),
                        },
                    }
                })
                .map_err(BackendError::from)
                .map_err(|e| {
                    metrics2.get_error_count();
                    metrics2.get_timer_stop(timer);
                    ClusterResult {
                        result: e,
                        node: n2,
                    }
                })
            }
            .boxed();
            Get(task)
        }

        #[allow(dead_code)]
        pub async fn ping(&mut self) -> PingResult {
            let mut client = self.client.clone();
            let ping_res = timeout(self.operation_timeout, client.ping(Request::new(Null {})))
                .await
                .expect("client ping with timeout")
                .map(|_| ClusterResult {
                    node: self.node.clone(),
                    result: BackendPingResult {},
                });
            ping_res.map_err(|_| ClusterResult {
                node: self.node.clone(),
                result: BackendError::Timeout,
            })
        }
    }

    mock! {
        pub BobClient {
            async fn create(node: Node, operation_timeout: Duration, metrics: BobClientMetrics,
                    ) -> Result<Self, String>;
            fn put(&mut self, key: BobKey, d: &BobData, options: PutOptions) -> Put;
            fn get(&mut self, key: BobKey, options: GetOptions) -> Get;
            async fn ping(&mut self) -> PingResult;
            fn node(&self) -> &Node;
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
        pub use self::b_client::RealBobClient as BobClient;
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
    operation_timeout: Duration,
    buffer_bound: u16,
    metrics: Arc<dyn MetricsContainerBuilder + Send + Sync>,
}

impl BobClientFactory {
    pub fn new(
        operation_timeout: Duration,
        buffer_bound: u16,
        metrics: Arc<dyn MetricsContainerBuilder + Send + Sync>,
    ) -> Self {
        BobClientFactory {
            operation_timeout,
            buffer_bound,
            metrics,
        }
    }
    pub(crate) async fn produce(&self, node: Node) -> Result<BobClient, String> {
        let metrics = self.metrics.clone().get_metrics(&node.counter_display());
        BobClient::create(node, self.operation_timeout, metrics).await
    }
}

pub mod tests {
    use super::*;
    use crate::core_inner::{
        backend::{BackendPingResult, BackendPutResult},
        data::{BobData, BobMeta, ClusterResult, Node},
    };
    use futures::future::ready;

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
