pub mod b_client {
    use super::{DeleteResult, ExistResult, FactoryTlsConfig, GetResult, PingResult, PutResult};
    use crate::{
        data::{BobData, BobKey, BobMeta},
        error::Error,
        metrics::BobClient as BobClientMetrics,
        node::{Node, Output as NodeOutput},
    };
    use bob_grpc::{
        bob_api_client::BobApiClient, Blob, BlobKey, BlobMeta, DeleteOptions, DeleteRequest,
        ExistRequest, GetOptions, GetRequest, Null, PutOptions, PutRequest,
    };
    use mockall::mock;
    use std::{
        fmt::{Debug, Formatter, Result as FmtResult},
        time::Duration,
        sync::{Arc, atomic::{AtomicUsize, Ordering},},
    };
    use tonic::{
        metadata::MetadataValue,
        transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
        Request, Status, Code,
    };

    /// Client for interaction with bob backend
    #[derive(Clone)]
    pub struct BobClient {
        node: Node,
        operation_timeout: Duration,
        client: BobApiClient<Channel>,
        metrics: BobClientMetrics,
        auth_header: String,
        err_count: Arc<AtomicUsize>,
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
            tls_config: Option<&FactoryTlsConfig>,
        ) -> Result<Self, String> {
            let mut endpoint = Endpoint::from(node.get_uri());
            if let Some(tls_config) = tls_config {
                let cert = Certificate::from_pem(&tls_config.ca_cert);
                let tls_config = ClientTlsConfig::new()
                    .domain_name(&tls_config.tls_domain_name)
                    .ca_certificate(cert);
                endpoint = endpoint.tls_config(tls_config).expect("client tls");
            }
            endpoint = endpoint.tcp_nodelay(true);

            let client = BobApiClient::connect(endpoint)
                .await
                .map_err(|e| e.to_string())?;

            let auth_header = format!("InterNode {}", base64::encode(local_node_name));

            Ok(Self {
                node,
                operation_timeout,
                client,
                metrics,
                auth_header,
                err_count: Arc::default(),
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
                    if self.error_count() > 0 {
                        self.reset_error_count();
                    }
                    Ok(NodeOutput::new(node_name, ()))
                }
                Err(e) => {
                    if is_network_error(&e) {
                        self.increase_error();
                    }
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
                    if self.error_count() > 0 {
                        self.reset_error_count();
                    }
                    Ok(NodeOutput::new(node_name.clone(), inner))
                }
                Err(e) => {
                    if is_network_error(&e) {
                        self.increase_error();
                    }
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
            self.metrics.exist_timer_stop(timer);
            match exist_response {
                Ok(response) => {
                    if self.error_count() > 0 {
                        self.reset_error_count();
                    }
                    Ok(NodeOutput::new(self.node.name().to_owned(), response.into_inner().exist))
                },
                Err(error) => {
                    if is_network_error(&error) {
                        self.increase_error();
                    }
                    self.metrics.exist_error_count();
                    Err(NodeOutput::new(self.node.name().to_owned(), error.into()))
                },
            }
        }

        pub async fn delete(&self, key: BobKey, meta: BobMeta, options: DeleteOptions) -> DeleteResult {
            let mut client = self.client.clone();
            self.metrics.delete_count();
            let timer = BobClientMetrics::start_timer();
            let message = DeleteRequest {
                key: Some(BlobKey { key: key.into() }),
                meta: Some(BlobMeta { timestamp: meta.timestamp() }),
                options: Some(options),
            };
            let mut req = Request::new(message);
            self.set_credentials(&mut req);
            self.set_timeout(&mut req);
            let res = client.delete(req).await;
            self.metrics.delete_timer_stop(timer);
            match res {
                Ok(_) => {
                    if self.error_count() > 0 {
                        self.reset_error_count();
                    }
                    Ok(NodeOutput::new(self.node().name().to_owned(), ()))
                },
                Err(e) => {
                    if is_network_error(&e) {
                        self.increase_error();
                    }
                    self.metrics.delete_error_count();
                    Err(NodeOutput::new(self.node().name().to_owned(), e.into()))
                }
            }
        }

        fn reset_error_count(&self) {
            self.err_count.store(0, Ordering::Relaxed);
        }

        fn increase_error(&self) -> usize {
            self.err_count.fetch_add(1, Ordering::Relaxed)
        }

        pub fn error_count(&self) -> usize {
            self.err_count.load(Ordering::Relaxed)
        }

        fn set_credentials<T>(&self, req: &mut Request<T>) {
            let val = MetadataValue::from_str(&self.auth_header)
                .expect("failed to create metadata value from node name");
            req.metadata_mut().insert("authorization", val);
        }

        fn set_timeout<T>(&self, r: &mut Request<T>) {
            r.set_timeout(self.operation_timeout);
        }
    }

    fn is_network_error(status: &Status) -> bool {
        status.code() == Code::Unavailable
    }

    mock! {
        pub BobClient {
            pub async fn create<'a>(node: Node, operation_timeout: Duration, metrics: BobClientMetrics, local_node_name: String, tls_config: Option<&'a FactoryTlsConfig>) -> Result<Self, String>;
            pub async fn put(&self, key: BobKey, d: BobData, options: PutOptions) -> PutResult;
            pub async fn get(&self, key: BobKey, options: GetOptions) -> GetResult;
            pub async fn ping(&self) -> PingResult;
            pub fn node(&self) -> &Node;
            pub async fn exist(&self, keys: Vec<BobKey>, options: GetOptions) -> ExistResult;
            pub async fn delete(&self, key: BobKey, meta: BobMeta, options: DeleteOptions) -> DeleteResult;
            pub fn increase_error(&self) -> usize;
            pub fn reset_error_count(&self);
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

#[derive(Clone)]
pub struct FactoryTlsConfig {
    pub tls_domain_name: String,
    pub ca_cert: Vec<u8>,
}

/// Bob metrics factory
#[derive(Clone)]
pub struct Factory {
    operation_timeout: Duration,
    metrics: Arc<dyn MetricsContainerBuilder + Send + Sync>,
    local_node_name: String,
    tls_config: Option<FactoryTlsConfig>,
}

impl Factory {
    /// Creates new instance of the [`Factory`]
    #[must_use]
    pub fn new(
        operation_timeout: Duration,
        metrics: Arc<dyn MetricsContainerBuilder + Send + Sync>,
        local_node_name: String,
        tls_config: Option<FactoryTlsConfig>,
    ) -> Self {
        Factory {
            operation_timeout,
            metrics,
            local_node_name,
            tls_config,
        }
    }
    pub async fn produce(&self, node: Node) -> Result<BobClient, String> {
        let metrics = self.metrics.clone().get_metrics();
        BobClient::create(
            node,
            self.operation_timeout,
            metrics,
            self.local_node_name.clone(),
            self.tls_config.as_ref(),
        )
        .await
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
