pub mod b_client {
    use super::{DeleteResult, ExistResult, FactoryTlsConfig, GetResult, PingResult, PutResult};
    use crate::{
        data::{BobData, BobKey, BobMeta},
        error::Error,
        metrics::BobClient as BobClientMetrics,
        node::{Node, NodeName, Output as NodeOutput},
    };
    use bob_grpc::{
        bob_api_client::BobApiClient, Blob, BlobKey, BlobMeta, DeleteOptions, DeleteRequest,
        ExistRequest, GetOptions, GetRequest, Null, PutOptions, PutRequest,
    };
    use mockall::mock;
    use std::{
        fmt::{Debug, Formatter, Result as FmtResult},
        time::Duration,
    };
    use tonic::{
        metadata::MetadataValue,
        transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
        Request,
    };

    /// Client for interaction with bob backend
    /// Clone implementation was removed, because struct is large. Use Arc to store copies
    pub struct BobClient {
        client: BobApiClient<Channel>,
        
        target_node_name: NodeName,
        target_node_address: String,
        local_node_name: String,

        operation_timeout: Duration,
        auth_header: String,
        metrics: BobClientMetrics,
    }

    impl BobClient {
        /// Creates [`BobClient`] instance
        /// # Errors
        /// Fails if can't connect to endpoint
        #[allow(dead_code)]
        pub async fn create(
            node: &Node,
            operation_timeout: Duration,
            metrics: BobClientMetrics,
            local_node_name: String,
            tls_config: Option<&FactoryTlsConfig>,
        ) -> Result<Self, String> 
        {
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

            let auth_header = format!("InterNode {}", base64::encode(local_node_name.clone()));

            Ok(Self {
                client,
                target_node_name: node.name().clone(),
                target_node_address: node.address().to_string(), 
                local_node_name: local_node_name, 
                operation_timeout: operation_timeout, 
                auth_header: auth_header, 
                metrics: metrics
            })
        }

        // Getters
        #[allow(dead_code)]
        pub fn target_node_name(&self) -> &str {
            &self.target_node_name
        }
        #[allow(dead_code)]
        pub fn target_node_address(&self) -> &str {
            &self.target_node_address
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

            let node_name = self.target_node_name.to_owned();
            let mut client = self.client.clone();
            
            self.metrics.put_count();
            let timer = BobClientMetrics::start_timer();

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
            let message = GetRequest {
                key: Some(BlobKey { key: key.into() }),
                options: Some(options),
            };
            let mut req = Request::new(message);
            self.set_credentials(&mut req);
            self.set_timeout(&mut req);

            let node_name = self.target_node_name.to_owned();
            let mut client = self.client.clone();

            self.metrics.get_count();
            let timer = BobClientMetrics::start_timer();

            match client.get(req).await {
                Ok(data) => {
                    self.metrics.get_timer_stop(timer);
                    let ans = data.into_inner();
                    let meta = BobMeta::new(ans.meta.expect("get blob meta").timestamp);
                    let inner = BobData::new(ans.data, meta);
                    Ok(NodeOutput::new(node_name, inner))
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
            let mut req = Request::new(Null {});
            self.set_credentials(&mut req);
            self.set_node_name(&mut req);
            self.set_timeout(&mut req);

            let node_name = self.target_node_name.to_owned();
            let mut client = self.client.clone();

            match client.ping(req).await {
                Ok(_) => Ok(NodeOutput::new(node_name, ())),
                Err(e) => Err(NodeOutput::new(node_name, Error::from(e))),
            }
        }

        #[allow(dead_code)]
        pub async fn exist(&self, keys: Vec<BobKey>, options: GetOptions) -> ExistResult {
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

            let node_name = self.target_node_name.to_owned();
            let mut client = self.client.clone();

            self.metrics.exist_count();
            let timer = BobClientMetrics::start_timer();

            match client.exist(req).await {
                Ok(response) => {
                    self.metrics.exist_timer_stop(timer);
                    Ok(NodeOutput::new(node_name, response.into_inner().exist))
                },
                Err(error) => {
                    self.metrics.exist_timer_stop(timer);
                    self.metrics.exist_error_count();
                    Err(NodeOutput::new(node_name, error.into()))
                }
            }
        }


        pub async fn delete(&self, key: BobKey, meta: BobMeta, options: DeleteOptions) -> DeleteResult {
            let message = DeleteRequest {
                key: Some(BlobKey { key: key.into() }),
                meta: Some(BlobMeta { timestamp: meta.timestamp() }),
                options: Some(options),
            };
            let mut req = Request::new(message);
            self.set_credentials(&mut req);
            self.set_timeout(&mut req);

            let node_name = self.target_node_name.to_owned();
            let mut client = self.client.clone();

            self.metrics.delete_count();
            let timer = BobClientMetrics::start_timer();

            match client.delete(req).await {
                Ok(_) => {
                    self.metrics.delete_timer_stop(timer);
                    Ok(NodeOutput::new(node_name, ()))
                },
                Err(error) => {
                    self.metrics.delete_timer_stop(timer);
                    self.metrics.delete_error_count();
                    Err(NodeOutput::new(node_name, error.into()))
                }
            }
        }

        fn set_credentials<T>(&self, req: &mut Request<T>) {
            let val = MetadataValue::from_str(&self.auth_header)
                .expect("failed to create metadata value from authorization");
            req.metadata_mut().insert("authorization", val);
        }

        fn set_node_name<T>(&self, r: &mut Request<T>) {
            let val = MetadataValue::from_str(&self.local_node_name)
                .expect("failed to create metadata value from node name");
            r.metadata_mut().insert("node_name", val);
        }

        fn set_timeout<T>(&self, r: &mut Request<T>) {
            r.set_timeout(self.operation_timeout);
        }
    }

    mock! {
        pub BobClient {
            pub async fn create<'a>(node: &Node, operation_timeout: Duration, metrics: BobClientMetrics, local_node_name: String, tls_config: Option<&'a FactoryTlsConfig>) -> Result<Self, String>;
            pub async fn put(&self, key: BobKey, d: BobData, options: PutOptions) -> PutResult;
            pub async fn get(&self, key: BobKey, options: GetOptions) -> GetResult;
            pub async fn ping(&self) -> PingResult;
            pub fn node(&self) -> &Node;
            pub async fn exist(&self, keys: Vec<BobKey>, options: GetOptions) -> ExistResult;
            pub async fn delete(&self, key: BobKey, meta: BobMeta, options: DeleteOptions) -> DeleteResult;
        }
        impl Clone for BobClient {
            fn clone(&self) -> Self;
        }
    }

    impl Debug for BobClient {
        fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
            f.debug_struct("RealBobClient")
                .field("client", &"BobApiClient<Channel>")
                .field("target_node_name", &self.target_node_name())
                .field("target_node_address", &self.target_node_address())
                .field("local_node_name", &self.local_node_name)
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
    pub async fn produce(&self, node: &Node) -> Result<BobClient, String> {
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
