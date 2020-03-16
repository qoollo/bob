pub(crate) mod b_client {
    use super::super::prelude::*;
    use super::PingResult;
    use super::{Exist, ExistResult, Get, Put};
    use crate::grpc::{Blob, BlobMeta};
    use crate::grpc::{GetOptions, GetRequest, PutOptions, PutRequest};
    use mockall::mock;
    use std::time::Duration;
    use tonic::Request;

    /// Client for interaction with bob backend
    #[derive(Clone)]
    pub struct RealBobClient {
        node: Node,
        operation_timeout: Duration,
        client: BobApiClient<Channel>,
        metrics: BobClientMetrics,
    }

    impl RealBobClient {
        /// Creates [`BobClient`] instance
        /// # Errors
        /// Fails if can't connect to endpoint
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
        #[must_use]
        pub(crate) fn node(&self) -> &Node {
            &self.node
        }

        #[allow(dead_code)]
        pub(crate) fn put(&mut self, key: BobKey, d: &BobData, options: PutOptions) -> Put {
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
        pub(crate) fn get(&mut self, key: BobKey, options: GetOptions) -> Get {
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
        pub(crate) async fn ping(&mut self) -> PingResult {
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

        #[allow(dead_code)]
        pub(crate) fn exist(&mut self, keys: Vec<BobKey>, options: GetOptions) -> Exist {
            let node = self.node.clone();
            let mut client = self.client.clone();
            Exist(Box::pin(async move {
                let exist_response = client
                    .exist(Request::new(ExistRequest {
                        keys: keys.into_iter().map(|k| BlobKey { key: k.key }).collect(),
                        options: Some(options),
                    }))
                    .await;
                Self::get_exist_result(node, exist_response)
            }))
        }

        fn get_exist_result(
            node: Node,
            exist_response: Result<Response<ExistResponse>, Status>,
        ) -> ExistResult {
            match exist_response {
                Ok(response) => Ok(ClusterResult {
                    node,
                    result: BackendExistResult {
                        exist: response.into_inner().exist,
                    },
                }),
                Err(error) => Err(ClusterResult {
                    node,
                    result: BackendError::from(error),
                }),
            }
        }
    }

    mock! {
        pub(crate) BobClient {
            async fn create(node: Node, operation_timeout: Duration, metrics: BobClientMetrics,
                    ) -> Result<Self, String>;
            fn put(&mut self, key: BobKey, d: &BobData, options: PutOptions) -> Put;
            fn get(&mut self, key: BobKey, options: GetOptions) -> Get;
            async fn ping(&mut self) -> PingResult;
            fn node(&self) -> &Node;
            fn exist(&mut self, keys: Vec<BobKey>, options: GetOptions) -> Exist;
        }
        trait Clone {
            fn clone(&self) -> Self;
        }
    }

    impl Debug for RealBobClient {
        fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
            f.debug_struct("RealBobClient")
                .field("client", &"BobApiClient<Channel>")
                .field("metrics", &self.metrics)
                .field("node", &self.node)
                .field("operation_timeout", &self.operation_timeout)
                .finish()
        }
    }
}

cfg_if! {
    if #[cfg(test)] {
        pub(crate) use self::b_client::MockBobClient as BobClient;
    } else {
        pub use self::b_client::RealBobClient as BobClient;
    }
}

use super::prelude::*;

pub(crate) type PutResult = Result<ClusterResult<BackendPutResult>, ClusterResult<BackendError>>;
pub(crate) struct Put(pub Pin<Box<dyn Future<Output = PutResult> + Send>>);

pub(crate) type GetResult = Result<ClusterResult<BackendGetResult>, ClusterResult<BackendError>>;
pub(crate) struct Get(pub Pin<Box<dyn Future<Output = GetResult> + Send>>);

pub(crate) type PingResult = Result<ClusterResult<BackendPingResult>, ClusterResult<BackendError>>;

pub(crate) type ExistResult =
    Result<ClusterResult<BackendExistResult>, ClusterResult<BackendError>>;
pub(crate) struct Exist(pub Pin<Box<dyn Future<Output = ExistResult> + Send>>);

/// Bob metrics factory
#[derive(Clone)]
pub struct Factory {
    operation_timeout: Duration,
    metrics: Arc<dyn MetricsContainerBuilder + Send + Sync>,
}

impl Factory {
    /// Creates new instance of the [`Factory`]
    #[must_use]
    pub fn new(
        operation_timeout: Duration,
        metrics: Arc<dyn MetricsContainerBuilder + Send + Sync>,
    ) -> Self {
        Factory {
            operation_timeout,
            metrics,
        }
    }
    pub(crate) async fn produce(&self, node: Node) -> Result<BobClient, String> {
        let metrics = self.metrics.clone().get_metrics(&node.counter_display());
        BobClient::create(node, self.operation_timeout, metrics).await
    }
}

impl Debug for Factory {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Factory")
            .field("operation_timeout", &self.operation_timeout)
            .field("metrics", &"<dyn MetricsContainerBuilder>")
            .finish()
    }
}

mod tests {
    use super::*;
    use crate::core::{
        backend::{BackendPingResult, BackendPutResult},
        data::{BobData, BobMeta, ClusterResult, Node},
    };
    use futures::future::ready;

    pub(crate) fn ping_ok(node: Node) -> PingResult {
        Ok(ClusterResult {
            node,
            result: BackendPingResult {},
        })
    }
    pub(crate) fn ping_err(node: Node) -> PingResult {
        Err(ClusterResult {
            node,
            result: BackendError::Internal,
        })
    }

    pub(crate) fn put_ok(node: Node) -> Put {
        Put({
            ready(Ok(ClusterResult {
                node,
                result: BackendPutResult {},
            }))
            .boxed()
        })
    }

    pub(crate) fn put_err(node: Node) -> Put {
        Put({
            ready(Err(ClusterResult {
                node,
                result: BackendError::Internal,
            }))
            .boxed()
        })
    }

    pub(crate) fn get_ok(node: Node, timestamp: i64) -> Get {
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

    pub(crate) fn get_err(node: Node) -> Get {
        Get({
            ready(Err(ClusterResult {
                node,
                result: BackendError::Internal,
            }))
            .boxed()
        })
    }
}
