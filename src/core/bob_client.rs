#[allow(clippy::ptr_arg)] // requires to avoid warnings on mock macro

pub(crate) mod b_client {
    use super::super::prelude::*;
    use super::{ExistResult, GetResult, PingResult, PutResult};
    use mockall::mock;

    /// Client for interaction with bob backend
    #[derive(Clone)]
    pub(crate) struct RealBobClient {
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
        pub(crate) async fn create(
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
        pub(crate) async fn put(&self, key: BobKey, d: BobData, options: PutOptions) -> PutResult {
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
            let timer = self.metrics.put_timer();
            let mut client = self.client.clone();
            let node_name = self.node.name().to_owned();
            let future = client.put(request).map(|res| {
                res.expect("client put request");
                self.metrics.put_timer_stop(timer);
            });
            if timeout(self.operation_timeout, future).await.is_ok() {
                Ok(NodeOutput::new(node_name, ()))
            } else {
                self.metrics.put_error_count();
                self.metrics.put_timer_stop(timer);
                Err(NodeOutput::new(node_name, BackendError::Timeout))
            }
        }

        #[allow(dead_code)]
        pub(crate) async fn get(&self, key: BobKey, options: GetOptions) -> GetResult {
            let node_name = self.node.name().to_owned();
            let mut client = self.client.clone();
            self.metrics.get_count();
            let timer = self.metrics.get_timer();

            let message = GetRequest {
                key: Some(BlobKey { key }),
                options: Some(options),
            };
            let req = Request::new(message);
            let res = timeout(self.operation_timeout, client.get(req))
                .await
                .map_err(|_| NodeOutput::new(node_name.clone(), BackendError::Timeout))?;
            res.map(|r| {
                self.metrics.get_timer_stop(timer);
                let ans = r.into_inner();
                let meta = BobMeta::new(ans.meta.expect("get blob meta").timestamp);
                let inner = BobData::new(ans.data, meta);
                NodeOutput::new(node_name.clone(), inner)
            })
            .map_err(|e| {
                self.metrics.get_error_count();
                self.metrics.get_timer_stop(timer);
                NodeOutput::new(node_name, BackendError::from(e))
            })
        }

        #[allow(dead_code)]
        pub(crate) async fn ping(&self) -> PingResult {
            let mut client = self.client.clone();
            timeout(self.operation_timeout, client.ping(Request::new(Null {})))
                .await
                .expect("client ping with timeout")
                .map(|_| NodeOutput::new(self.node.name().to_owned(), ()))
                .map_err(|_| NodeOutput::new(self.node.name().to_owned(), BackendError::Timeout))
        }

        #[allow(dead_code)]
        pub(crate) async fn exist(&self, keys: Vec<BobKey>, options: GetOptions) -> ExistResult {
            let mut client = self.client.clone();
            let keys = keys.into_iter().map(|key| BlobKey { key }).collect();
            let message = ExistRequest {
                keys,
                options: Some(options),
            };
            let req = Request::new(message);
            let exist_response = client.exist(req).await;
            Self::get_exist_result(self.node.name().to_owned(), exist_response)
        }

        fn get_exist_result(
            node_name: String,
            exist_response: Result<Response<ExistResponse>, Status>,
        ) -> ExistResult {
            match exist_response {
                Ok(response) => Ok(NodeOutput::new(node_name, response.into_inner().exist)),
                Err(error) => Err(NodeOutput::new(node_name, BackendError::from(error))),
            }
        }
    }

    mock! {
        pub(crate) BobClient {
            async fn create(node: Node, operation_timeout: Duration, metrics: BobClientMetrics) -> Result<Self, String>;
            async fn put(&self, key: BobKey, d: BobData, options: PutOptions) -> PutResult;
            async fn get(&self, key: BobKey, options: GetOptions) -> GetResult;
            async fn ping(&self) -> PingResult;
            fn node(&self) -> &Node;
            async fn exist(&self, keys: Vec<BobKey>, options: GetOptions) -> ExistResult;
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
        pub(crate) use self::b_client::RealBobClient as BobClient;
    }
}

use super::prelude::*;

pub(crate) type PutResult = Result<NodeOutput<()>, NodeOutput<BackendError>>;

pub(crate) type GetResult = Result<NodeOutput<BobData>, NodeOutput<BackendError>>;

pub(crate) type PingResult = Result<NodeOutput<()>, NodeOutput<BackendError>>;

pub(crate) type ExistResult = Result<NodeOutput<Vec<bool>>, NodeOutput<BackendError>>;

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
