pub mod b_client {
    use super::{ExistResult, GetResult, PingResult, PutResult};
    use crate::{
        data::{BobData, BobKey, BobMeta},
        error::Error,
        metrics::BobClient as BobClientMetrics,
        node::{Node, Output as NodeOutput},
    };
    use bob_grpc::{
        bob_api_client::BobApiClient, Blob, BlobKey, BlobMeta, ExistRequest, ExistResponse,
        GetOptions, GetRequest, Null, PutOptions, PutRequest,
    };
    use mockall::mock;
    use std::{
        fmt::{Debug, Formatter, Result as FmtResult},
        time::Duration,
    };
    use tokio::time::timeout;
    use tonic::{
        transport::{Channel, Endpoint},
        Request, Response, Status,
    };

    /// Client for interaction with bob backend
    #[derive(Clone)]
    pub struct BobClient {
        node: Node,
        operation_timeout: Duration,
        client: BobApiClient<Channel>,
        metrics: BobClientMetrics,
    }

    impl BobClient {
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
                operation_timeout,
                client,
                metrics,
            })
        }

        // Getters

        #[allow(dead_code)]
        #[must_use]
        pub fn node(&self) -> &Node {
            &self.node
        }

        #[allow(dead_code)]
        pub async fn put(&self, key: BobKey, d: BobData, options: PutOptions) -> PutResult {
            debug!("real client put called");
            let meta = BlobMeta {
                timestamp: d.meta().timestamp(),
            };
            let blob = Blob {
                meta: Some(meta),
                data: d.into_inner(),
            };
            let message = PutRequest {
                key: Some(BlobKey { key }),
                data: Some(blob),
                options: Some(options),
            };
            let request = Request::new(message);
            self.metrics.put_count();
            let timer = BobClientMetrics::start_timer();
            let mut client = self.client.clone();
            let node_name = self.node.name().to_owned();
            let future = client.put(request);
            if timeout(self.operation_timeout, future).await.is_ok() {
                self.metrics.put_timer_stop(timer);
                Ok(NodeOutput::new(node_name, ()))
            } else {
                self.metrics.put_error_count();
                self.metrics.put_timer_stop(timer);
                Err(NodeOutput::new(node_name, Error::timeout()))
            }
        }

        #[allow(dead_code)]
        pub async fn get(&self, key: BobKey, options: GetOptions) -> GetResult {
            let node_name = self.node.name().to_owned();
            let mut client = self.client.clone();
            self.metrics.get_count();
            let timer = BobClientMetrics::start_timer();

            let message = GetRequest {
                key: Some(BlobKey { key }),
                options: Some(options),
            };
            let request = Request::new(message);
            let result = timeout(self.operation_timeout, client.get(request)).await;
            match result {
                Ok(Ok(data)) => {
                    self.metrics.get_timer_stop(timer);
                    let ans = data.into_inner();
                    let meta = BobMeta::new(ans.meta.expect("get blob meta").timestamp);
                    let inner = BobData::new(ans.data, meta);
                    Ok(NodeOutput::new(node_name.clone(), inner))
                }
                Ok(Err(e)) => {
                    self.metrics.get_error_count();
                    self.metrics.get_timer_stop(timer);
                    Err(NodeOutput::new(node_name, Error::from(e)))
                }
                Err(_) => Err(NodeOutput::new(node_name.clone(), Error::timeout())),
            }
        }

        #[allow(dead_code)]
        pub async fn ping(&self) -> PingResult {
            let mut client = self.client.clone();
            let result = timeout(self.operation_timeout, client.ping(Request::new(Null {}))).await;
            match result {
                Ok(Ok(_)) => Ok(NodeOutput::new(self.node.name().to_owned(), ())),
                Ok(Err(e)) => Err(NodeOutput::new(self.node.name().to_owned(), Error::from(e))),
                Err(_) => {
                    warn!("node {} ping timeout, reset connection", self.node.name());
                    Err(NodeOutput::new(
                        self.node.name().to_owned(),
                        Error::timeout(),
                    ))
                }
            }
        }

        #[allow(dead_code)]
        pub async fn exist(&self, keys: Vec<BobKey>, options: GetOptions) -> ExistResult {
            let mut client = self.client.clone();
            self.metrics.exist_count();
            let timer = BobClientMetrics::start_timer();
            let keys = keys.into_iter().map(|key| BlobKey { key }).collect();
            let message = ExistRequest {
                keys,
                options: Some(options),
            };
            let req = Request::new(message);
            let exist_response = client.exist(req).await;
            let result = Self::get_exist_result(self.node.name().to_owned(), exist_response);
            self.metrics.exist_timer_stop(timer);
            if result.is_err() {
                self.metrics.exist_error_count();
            }
            result
        }

        fn get_exist_result(
            node_name: String,
            exist_response: Result<Response<ExistResponse>, Status>,
        ) -> ExistResult {
            match exist_response {
                Ok(response) => Ok(NodeOutput::new(node_name, response.into_inner().exist)),
                Err(error) => Err(NodeOutput::new(node_name, Error::from(error))),
            }
        }
    }

    mock! {
        pub BobClient {
            pub async fn create(node: Node, operation_timeout: Duration, metrics: BobClientMetrics) -> Result<Self, String>;
            pub async fn put(&self, key: BobKey, d: BobData, options: PutOptions) -> PutResult;
            pub async fn get(&self, key: BobKey, options: GetOptions) -> GetResult;
            pub async fn ping(&self) -> PingResult;
            pub fn node(&self) -> &Node;
            pub async fn exist(&self, keys: Vec<BobKey>, options: GetOptions) -> ExistResult;
        }
        impl Clone for BobClient {
            fn clone(&self) -> Self;
        }
    }

    impl Debug for BobClient {
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

use crate::{
    data::BobData,
    error::Error,
    metrics::ContainerBuilder as MetricsContainerBuilder,
    node::{Node, Output as NodeOutput},
};
use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    sync::Arc,
    time::Duration,
};

// cfg_if! {
//     if #[cfg(test)] {
//         pub use self::b_client::MockBobClient as BobClient;
// } else {
pub use self::b_client::BobClient;
//     }
// }

pub type PutResult = Result<NodeOutput<()>, NodeOutput<Error>>;

pub type GetResult = Result<NodeOutput<BobData>, NodeOutput<Error>>;

pub type PingResult = Result<NodeOutput<()>, NodeOutput<Error>>;

pub type ExistResult = Result<NodeOutput<Vec<bool>>, NodeOutput<Error>>;

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
    pub async fn produce(&self, node: Node) -> Result<BobClient, String> {
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
