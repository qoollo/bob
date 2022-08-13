pub mod b_client {
    use super::{DeleteResult, ExistResult, GetResult, PingResult, PutResult};
    use crate::{
        data::{BobData, BobKey, BobMeta},
        error::Error,
        metrics::BobClient as BobClientMetrics,
        node::{Node, Output as NodeOutput},
    };
    use bob_grpc::{
        bob_api_client::BobApiClient, Blob, BlobKey, BlobMeta, DeleteOptions, DeleteRequest,
        ExistRequest, ExistResponse, GetOptions, GetRequest, Null, PutOptions, PutRequest,
    };
    use mockall::mock;
    use std::{
        fmt::{Debug, Formatter, Result as FmtResult},
        time::Duration,
    };
    use tonic::{
        metadata::MetadataValue,
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
        local_node_name: String,
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
            local_node_name: String,
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
                local_node_name,
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
                key: Some(BlobKey { key: key.into() }),
                data: Some(blob),
                options: Some(options),
            };
            let mut req = Request::new(message);
            self.set_credentials(&mut req);
            self.set_timeout(&mut req);
            self.metrics.put_count();
            let timer = BobClientMetrics::start_timer();
            let mut client = self.client.clone();
            let node_name = self.node.name().to_owned();
            match client.put(req).await {
                Ok(_) => {
                    self.metrics.put_timer_stop(timer);
                    Ok(NodeOutput::new(node_name, ()))
                }
                Err(e) => {
                    self.metrics.put_error_count();
                    self.metrics.put_timer_stop(timer);
                    Err(NodeOutput::new(node_name, e.into()))
                }
            }
        }

        #[allow(dead_code)]
        pub async fn get(&self, key: BobKey, options: GetOptions) -> GetResult {
            let node_name = self.node.name().to_owned();
            let mut client = self.client.clone();
            self.metrics.get_count();
            let timer = BobClientMetrics::start_timer();

            let message = GetRequest {
                key: Some(BlobKey { key: key.into() }),
                options: Some(options),
            };
            let mut req = Request::new(message);
            self.set_credentials(&mut req);
            self.set_timeout(&mut req);
            match client.get(req).await {
                Ok(data) => {
                    self.metrics.get_timer_stop(timer);
                    let ans = data.into_inner();
                    let meta = BobMeta::new(ans.meta.expect("get blob meta").timestamp);
                    let inner = BobData::new(ans.data, meta);
                    Ok(NodeOutput::new(node_name.clone(), inner))
                }
                Err(e) => {
                    self.metrics.get_error_count();
                    self.metrics.get_timer_stop(timer);
                    Err(NodeOutput::new(node_name, e.into()))
                }
            }
        }

        #[allow(dead_code)]
        pub async fn ping(&self) -> PingResult {
            let mut client = self.client.clone();
            let mut req = Request::new(Null {});
            self.set_credentials(&mut req);
            self.set_timeout(&mut req);
            match client.ping(req).await {
                Ok(_) => Ok(NodeOutput::new(self.node.name().to_owned(), ())),
                Err(e) => Err(NodeOutput::new(self.node.name().to_owned(), Error::from(e))),
            }
        }

        #[allow(dead_code)]
        pub async fn exist(&self, keys: Vec<BobKey>, options: GetOptions) -> ExistResult {
            let mut client = self.client.clone();
            self.metrics.exist_count();
            let timer = BobClientMetrics::start_timer();
            let keys = keys
                .into_iter()
                .map(|key| BlobKey { key: key.into() })
                .collect();
            let message = ExistRequest {
                keys,
                options: Some(options),
            };
            let mut req = Request::new(message);
            self.set_credentials(&mut req);
            self.set_timeout(&mut req);
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
                Err(error) => Err(NodeOutput::new(node_name, error.into())),
            }
        }

        pub async fn delete(&self, key: BobKey, options: DeleteOptions) -> DeleteResult {
            let mut client = self.client.clone();
            self.metrics.delete_count();
            let timer = BobClientMetrics::start_timer();
            let message = DeleteRequest {
                key: Some(BlobKey { key: key.into() }),
                options: Some(options),
            };
            let mut req = Request::new(message);
            self.set_credentials(&mut req);
            self.set_timeout(&mut req);
            let res = client.delete(req).await;
            self.metrics.delete_timer_stop(timer);
            res.map(|_| NodeOutput::new(self.node().name().to_owned(), ()))
                .map_err(|e| {
                    self.metrics.delete_error_count();
                    NodeOutput::new(self.node().name().to_owned(), e.into())
                })
        }

        fn set_credentials<T>(&self, req: &mut Request<T>) {
            let val = MetadataValue::from_str(&self.local_node_name)
                .expect("failed to create metadata value from node name");
            req.metadata_mut().insert("node_name", val);
        }

        fn set_timeout<T>(&self, r: &mut Request<T>) {
            r.set_timeout(self.operation_timeout);
        }
    }

    mock! {
        pub BobClient {
            pub async fn create(node: Node, operation_timeout: Duration, metrics: BobClientMetrics, local_node_name: String) -> Result<Self, String>;
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

cfg_if::cfg_if! {
    if #[cfg(feature = "testing")] {
        pub use self::b_client::MockBobClient as BobClient;
    } else {
        pub use self::b_client::BobClient;
    }
}

type NodeResult<T> = Result<NodeOutput<T>, NodeOutput<Error>>;

pub type PutResult = NodeResult<()>;

pub type GetResult = NodeResult<BobData>;

pub type PingResult = NodeResult<()>;

pub type ExistResult = NodeResult<Vec<bool>>;

pub type DeleteResult = NodeResult<()>;

/// Bob metrics factory
#[derive(Clone)]
pub struct Factory {
    operation_timeout: Duration,
    metrics: Arc<dyn MetricsContainerBuilder + Send + Sync>,
    local_node_name: String,
}

impl Factory {
    /// Creates new instance of the [`Factory`]
    #[must_use]
    pub fn new(
        operation_timeout: Duration,
        metrics: Arc<dyn MetricsContainerBuilder + Send + Sync>,
        local_node_name: String,
    ) -> Self {
        Factory {
            operation_timeout,
            metrics,
            local_node_name,
        }
    }
    pub async fn produce(&self, node: Node) -> Result<BobClient, String> {
        let metrics = self.metrics.clone().get_metrics(&node.counter_display());
        BobClient::create(node, self.operation_timeout, metrics, self.local_node_name.clone()).await
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
