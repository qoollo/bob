use super::{
    cluster::{Cluster as ClusterConfig, Node as ClusterNodeConfig},
    node::Node as NodeConfig,
    reader::{Validatable, YamlBobConfig},
};
use bob_access::AuthenticationType;
use crate::data::DiskPath;
use futures::Future;
use humantime::Duration as HumanDuration;
use std::{
    env::VarError,
    fmt::Debug,
    net::SocketAddr,
    sync::{atomic::AtomicBool, Mutex},
    time::Duration,
};
use std::{net::IpAddr, sync::atomic::Ordering};
use std::{net::Ipv4Addr, sync::Arc};
use tokio::time::sleep;

use ubyte::ByteUnit;

const AIO_FLAG_ORDERING: Ordering = Ordering::Relaxed;

const PLACEHOLDER: &str = "~";
const TMP_DIR_ENV_VARS: [&str; 3] = ["TMP", "TEMP", "TMPDIR"];

pub const LOCAL_ADDRESS: &str = "{local_address}";
pub const NODE_NAME: &str = "{node_name}";
pub const METRICS_NAME: &str = "{metrics_name}";

const FIELD_PLACEHOLDER: &str = "_";

/// Contains settings for pearl backend.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct BackendSettings {
    root_dir_name: String,
    alien_root_dir_name: String,
    timestamp_period: String,
    create_pearl_wait_delay: String,
}

impl Validatable for BackendSettings {
    fn validate(&self) -> Result<(), String> {
        self.check_unset()?;
        if self.root_dir_name.is_empty() {
            let msg = "field \'root_dir_name\' for backend settings config is empty".to_string();
            error!("{}", msg);
            return Err(msg);
        }

        if self.alien_root_dir_name.is_empty() {
            let msg =
                "field 'alien_root_dir_name' for 'backend settings config' is empty".to_string();
            error!("{}", msg);
            return Err(msg);
        }

        if let Err(e) = self.timestamp_period.parse::<HumanDuration>() {
            let msg = format!(
                "field 'timestamp_period' for 'backend settings config' is not valid: {}",
                e
            );
            error!("{}", msg);
            return Err(msg);
        }
        let period = chrono::Duration::from_std(self.timestamp_period())
            .expect("smth wrong with time: {:?}, error: {}");
        if period > chrono::Duration::weeks(1) {
            let msg = "field 'timestamp_period' for 'backend settings config' is greater then week"
                .to_string();
            error!("{}", msg);
            return Err(msg);
        }

        if self
            .create_pearl_wait_delay
            .parse::<HumanDuration>()
            .is_err()
        {
            let msg = "field \'create_pearl_wait_delay\' for backend settings config is not valid"
                .to_string();
            error!("{}", msg);
            Err(msg)
        } else {
            Ok(())
        }
    }
}

impl BackendSettings {
    pub fn root_dir_name(&self) -> &str {
        &self.root_dir_name
    }

    pub fn alien_root_dir_name(&self) -> &str {
        &self.alien_root_dir_name
    }

    pub fn timestamp_period(&self) -> Duration {
        self.timestamp_period
            .parse::<HumanDuration>()
            .expect("parse humantime duration")
            .into()
    }

    pub fn create_pearl_wait_delay(&self) -> Duration {
        self.create_pearl_wait_delay
            .parse::<HumanDuration>()
            .expect("parse humantime duration")
            .into()
    }

    fn check_unset(&self) -> Result<(), String> {
        if self.alien_root_dir_name == PLACEHOLDER
            || self.create_pearl_wait_delay == PLACEHOLDER
            || self.root_dir_name == PLACEHOLDER
            || self.timestamp_period == PLACEHOLDER
        {
            let msg = "some of the fields present, but empty".to_string();
            error!("{}", msg);
            Err(msg)
        } else {
            Ok(())
        }
    }
}

/// Contains params for graphite metrics.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct MetricsConfig {
    name: Option<String>,
    #[serde(default = "MetricsConfig::default_prometheus_addr")]
    prometheus_addr: String,
    graphite_enabled: bool,
    graphite: Option<String>,
    prometheus_enabled: bool,
    prefix: Option<String>,
}

impl MetricsConfig {
    pub(crate) fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    pub(crate) fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub(crate) fn graphite_enabled(&self) -> bool {
        self.graphite_enabled
    }

    pub(crate) fn prometheus_enabled(&self) -> bool {
        self.prometheus_enabled
    }

    pub(crate) fn graphite(&self) -> Option<&str> {
        self.graphite.as_deref()
    }

    fn check_unset(&self) -> Result<(), String> {
        if self.graphite_enabled && self.graphite.is_none() {
            let msg = "graphite is enabled but no graphite address has been provided".to_string();
            error!("{}", msg);
            Err(msg)
        } else {
            Ok(())
        }
    }

    pub(crate) fn prometheus_addr(&self) -> &str {
        &self.prometheus_addr
    }

    pub(crate) fn default_prometheus_addr() -> String {
        "0.0.0.0:9000".to_owned()
    }

    fn check_optional_fields(&self) -> Result<(), String> {
        // Case, when field is set like `field: ''`
        let optional_fields = [self.name.as_deref(), self.prefix.as_deref()];
        if optional_fields
            .iter()
            .any(|field| field.map_or(false, str::is_empty))
        {
            debug!("one of optional fields for 'metrics config' is empty");
            Err("one of optional fields for 'metrics config' is empty".to_string())
        } else {
            Ok(())
        }
    }

    fn check_graphite_addr(&self) -> Result<(), String> {
        if !self.graphite_enabled {
            Ok(())
        } else if let Err(e) = self.graphite().unwrap().parse::<SocketAddr>() {
            let msg = format!("field 'graphite': {} for 'metrics config' is invalid", e);
            error!("{}", msg);
            Err(msg)
        } else {
            Ok(())
        }
    }

    fn check_graphite_prefix(prefix: &str) -> Result<(), String> {
        let invalid_char_predicate =
            |c| !(('a'..='z').contains(&c) || ('A'..='Z').contains(&c) || ("._".contains(c)));
        if prefix.starts_with('.')
            || prefix.ends_with('.')
                || prefix.contains("..")
                // check if there is '{', '}' or other invalid chars left
                || prefix.find(invalid_char_predicate).is_some()
        {
            let msg = "Graphite 'prefix' is invalid".to_string();
            error!("{}", msg);
            Err(msg)
        } else {
            Ok(())
        }
    }

    fn check_prefix(&self) -> Result<(), String> {
        self.prefix.as_ref().cloned().map_or(Ok(()), |pref| {
            let mut prefix = [LOCAL_ADDRESS, NODE_NAME]
                .iter()
                .fold(pref, |acc, field| acc.replace(field, FIELD_PLACEHOLDER));
            if self.name.is_some() {
                prefix = prefix.replace(METRICS_NAME, FIELD_PLACEHOLDER);
            }
            Self::check_graphite_prefix(&prefix)
        })
    }
}

impl Validatable for MetricsConfig {
    fn validate(&self) -> Result<(), String> {
        self.check_unset()?;
        self.check_optional_fields()?;
        self.check_graphite_addr()?;
        self.check_prefix()
    }
}

/// Contains params for detailed pearl configuration in pearl backend.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Pearl {
    #[serde(default = "Pearl::default_max_blob_size")]
    max_blob_size: ByteUnit,
    #[serde(default = "Pearl::default_max_data_in_blob")]
    max_data_in_blob: u64,
    #[serde(default = "Pearl::default_blob_file_name_prefix")]
    blob_file_name_prefix: String,
    #[serde(default = "Pearl::default_fail_retry_timeout")]
    fail_retry_timeout: String,
    #[serde(default = "Pearl::default_fail_retry_count")]
    fail_retry_count: u64,
    alien_disk: Option<String>,
    #[serde(default = "Pearl::default_allow_duplicates")]
    allow_duplicates: bool,
    settings: BackendSettings,
    #[serde(default = "Pearl::default_hash_chars_count")]
    hash_chars_count: u32,
    #[serde(default = "Pearl::default_enable_aio")]
    enable_aio: Arc<AtomicBool>,
    #[serde(default = "Pearl::default_disks_events_logfile")]
    disks_events_logfile: String,
    #[serde(default)]
    bloom_filter_max_buf_bits_count: Option<usize>,
}

impl Pearl {
    pub fn max_buf_bits_count(&self) -> Option<usize> {
        self.bloom_filter_max_buf_bits_count
    }

    pub fn alien_disk(&self) -> Option<&str> {
        self.alien_disk.as_deref()
    }

    fn default_fail_retry_timeout() -> String {
        "100ms".to_string()
    }

    pub fn fail_retry_timeout(&self) -> Duration {
        self.fail_retry_timeout
            .parse::<HumanDuration>()
            .expect("parse humantime duration")
            .into()
    }

    fn default_fail_retry_count() -> u64 {
        3
    }

    pub fn fail_retry_count(&self) -> u64 {
        self.fail_retry_count
    }

    pub fn settings(&self) -> &BackendSettings {
        &self.settings
    }

    fn prepare(&self) {
        self.fail_retry_timeout(); // TODO check unwrap
    }

    fn default_allow_duplicates() -> bool {
        true
    }

    pub fn allow_duplicates(&self) -> bool {
        self.allow_duplicates
    }

    fn default_blob_file_name_prefix() -> String {
        "bob".to_string()
    }

    pub fn blob_file_name_prefix(&self) -> &str {
        &self.blob_file_name_prefix
    }

    pub fn set_blob_file_name_prefix(&mut self, s: String) {
        self.blob_file_name_prefix = s;
    }

    fn default_max_data_in_blob() -> u64 {
        1_000_000
    }

    pub fn max_data_in_blob(&self) -> u64 {
        self.max_data_in_blob
    }

    fn default_max_blob_size() -> ByteUnit {
        ByteUnit::MB
    }

    pub fn max_blob_size(&self) -> u64 {
        self.max_blob_size.as_u64()
    }

    fn default_hash_chars_count() -> u32 {
        10
    }

    fn default_enable_aio() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(false))
    }

    pub fn disks_events_logfile(&self) -> &str {
        &self.disks_events_logfile
    }

    fn default_disks_events_logfile() -> String {
        use std::path::PathBuf;
        let mut tmp_dir = PathBuf::from(
            TMP_DIR_ENV_VARS
                .iter()
                .fold(Err(VarError::NotPresent), |acc, elem| {
                    std::env::var(elem).or(acc)
                })
                .unwrap_or_else(|_| "/tmp".to_owned()),
        );
        tmp_dir.push("bob_events.csv");
        tmp_dir.to_str().expect("Path is not UTF-8").to_owned()
    }

    pub fn hash_chars_count(&self) -> u32 {
        self.hash_chars_count
    }

    pub fn is_aio_enabled(&self) -> bool {
        self.enable_aio.load(AIO_FLAG_ORDERING)
    }

    pub fn set_aio(&self, new_value: bool) {
        self.enable_aio.store(new_value, AIO_FLAG_ORDERING);
    }

    fn check_unset(&self) -> Result<(), String> {
        if self.blob_file_name_prefix == PLACEHOLDER || self.fail_retry_timeout == PLACEHOLDER {
            let msg = "some of the fields present, but empty".to_string();
            error!("{}", msg);
            Err(msg)
        } else {
            Ok(())
        }
    }

    /// Helper for running provided function multiple times.
    /// # Errors
    /// Returns errors from provided fn.
    pub async fn try_multiple_times<F, T, E>(
        &self,
        f: F,
        error_prefix: &str,
        retry_delay: Duration,
    ) -> Result<T, E>
    where
        F: Fn() -> Result<T, E>,
        E: Debug,
    {
        let a = || async { f() };
        self.try_multiple_times_async(a, error_prefix, retry_delay)
            .await
    }

    pub async fn try_multiple_times_async<F, T, E, Fut>(
        &self,
        f: F,
        error_prefix: &str,
        retry_delay: Duration,
    ) -> Result<T, E>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: Debug,
    {
        let retry_count = self.fail_retry_count();
        for attempt in 0..retry_count {
            match f().await {
                Ok(value) => return Ok(value),
                Err(e) => {
                    error!(
                        "{}, attempt {}/{}, error {:?}",
                        error_prefix,
                        attempt + 1,
                        retry_count,
                        e
                    );
                    if attempt == retry_count - 1 {
                        return Err(e);
                    }
                }
            }
            sleep(retry_delay).await
        }
        unreachable!()
    }
}

impl Validatable for Pearl {
    fn validate(&self) -> Result<(), String> {
        self.check_unset()?;
        if self.fail_retry_timeout.parse::<HumanDuration>().is_err() {
            let msg = "field \'fail_retry_timeout\' for \'config\' is not valid".to_string();
            error!("{}", msg);
            Err(msg)
        } else {
            self.settings.validate()
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Copy)]
pub enum BackendType {
    InMemory = 0,
    Stub,
    Pearl,
}

/// Node configuration struct, stored in node.yaml.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Node {
    log_config: String,
    users_config: String,
    name: String,
    quorum: usize,
    operation_timeout: String,
    check_interval: String,
    #[serde(default = "NodeConfig::default_count_interval")]
    count_interval: String,
    cluster_policy: String,

    backend_type: String,
    pearl: Option<Pearl>,
    metrics: Option<MetricsConfig>,

    #[serde(skip)]
    bind_ref: Arc<Mutex<String>>,
    #[serde(skip)]
    disks_ref: Arc<Mutex<Vec<DiskPath>>>,

    cleanup_interval: String,
    open_blobs_soft_limit: Option<usize>,
    open_blobs_hard_limit: Option<usize>,
    bloom_filter_memory_limit: Option<ByteUnit>,
    index_memory_limit: Option<ByteUnit>,
    index_memory_limit_soft: Option<ByteUnit>,
    #[serde(default = "Node::default_init_par_degree")]
    init_par_degree: usize,
    #[serde(default = "Node::default_disk_access_par_degree")]
    disk_access_par_degree: usize,
    #[serde(default = "Node::default_http_api_port")]
    http_api_port: u16,
    #[serde(default = "Node::default_http_api_address")]
    http_api_address: IpAddr,
    bind_to_ip_address: Option<SocketAddr>,
    #[serde(default = "NodeConfig::default_holder_group_size")]
    holder_group_size: usize,

    #[serde(default = "NodeConfig::default_authentication_type")]
    authentication_type: AuthenticationType,

    #[serde(default = "NodeConfig::default_hostname_resolve_dur_ms")]
    hostname_resolve_dur_ms: u64,
}

impl NodeConfig {
    pub fn http_api_port(&self) -> u16 {
        self.http_api_port
    }

    pub fn http_api_address(&self) -> IpAddr {
        self.http_api_address
    }

    pub fn bind_to_ip_address(&self) -> Option<SocketAddr> {
        self.bind_to_ip_address
    }

    /// Get node name.
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn quorum(&self) -> usize {
        self.quorum
    }

    pub fn pearl(&self) -> &Pearl {
        self.pearl.as_ref().expect("get pearl config")
    }

    pub fn metrics(&self) -> &MetricsConfig {
        self.metrics.as_ref().expect("metrics config")
    }

    /// Get log config file path.
    pub fn log_config(&self) -> &str {
        &self.log_config
    }

    /// Get users config file path
    pub fn users_config(&self) -> &str {
        &self.users_config
    }

    pub fn cluster_policy(&self) -> &str {
        &self.cluster_policy
    }

    /// Get reference to bind address.
    pub fn bind(&self) -> Arc<Mutex<String>> {
        self.bind_ref.clone()
    }

    /// Get grpc request operation timeout, parsed from humantime format.
    pub fn operation_timeout(&self) -> Duration {
        self.operation_timeout
            .parse::<HumanDuration>()
            .expect("parse humantime duration")
            .into()
    }

    pub fn check_interval(&self) -> Duration {
        self.check_interval
            .parse::<HumanDuration>()
            .expect("parse humantime duration")
            .into()
    }

    fn default_count_interval() -> String {
        "10000ms".to_string()
    }

    pub fn count_interval(&self) -> Duration {
        self.count_interval
            .parse::<HumanDuration>()
            .expect("parse humantime duration")
            .into()
    }

    /// Get reference to collection of disks [`DiskPath`]
    pub fn disks(&self) -> Arc<Mutex<Vec<DiskPath>>> {
        self.disks_ref.clone()
    }

    pub fn backend_type(&self) -> BackendType {
        self.backend_result().expect("clone backend type")
    }

    pub fn authentication_type(&self) -> AuthenticationType {
        self.authentication_type
    }

    fn default_authentication_type() -> AuthenticationType {
        AuthenticationType::None
    }

    fn default_hostname_resolve_dur_ms() -> u64 {
        5000
    }

    pub fn hostname_resolve_dur_ms(&self) -> u64 {
        self.hostname_resolve_dur_ms
    }

    pub fn backend_result(&self) -> Result<BackendType, String> {
        match self.backend_type.as_str() {
            "in_memory" => Ok(BackendType::InMemory),
            "stub" => Ok(BackendType::Stub),
            "pearl" => Ok(BackendType::Pearl),
            value => Err(format!("unknown backend type: {}", value)),
        }
    }

    pub fn prepare(&self, node: &ClusterNodeConfig) -> Result<(), String> {
        {
            let mut lck = self.bind_ref.lock().expect("mutex");
            *lck = node.address().to_owned();
        }
        let t = node
            .disks()
            .iter()
            .map(|disk| DiskPath::new(disk.name().to_owned(), disk.path().to_owned()))
            .collect::<Vec<_>>();

        {
            let mut lck = self.disks_ref.lock().expect("mutex");
            *lck = t;
        }
        self.backend_result()?;

        if self.backend_type() == BackendType::Pearl {
            if let Some(pearl) = &self.pearl {
                pearl.prepare();
            }
        }
        Ok(())
    }

    pub fn cleanup_interval(&self) -> Duration {
        self.cleanup_interval
            .parse::<HumanDuration>()
            .expect("parse humantime duration")
            .into()
    }

    pub fn open_blobs_soft(&self) -> Option<usize> {
        self.open_blobs_soft_limit.and_then(|i| {
            if i == 0 {
                error!("soft open blobs limit can't be less than 1");
                None
            } else {
                Some(i)
            }
        })
    }

    pub fn hard_open_blobs(&self) -> Option<usize> {
        self.open_blobs_hard_limit.and_then(|i| {
            if i == 0 {
                error!("hard open blobs limit can't be less than 1");
                None
            } else {
                Some(i)
            }
        })
    }

    pub fn bloom_filter_memory_limit(&self) -> Option<usize> {
        self.bloom_filter_memory_limit
            .map(|bu| bu.as_u64() as usize)
    }

    pub fn index_memory_limit(&self) -> Option<usize> {
        self.index_memory_limit.map(|bu| bu.as_u64() as usize)
    }

    pub fn index_memory_limit_soft(&self) -> Option<usize> {
        self.index_memory_limit_soft.map(|bu| bu.as_u64() as usize)
    }

    #[inline]
    pub fn init_par_degree(&self) -> usize {
        self.init_par_degree
    }

    #[inline]
    pub fn disk_access_par_degree(&self) -> usize {
        self.disk_access_par_degree
    }

    #[inline]
    pub fn holder_group_size(&self) -> usize {
        self.holder_group_size
    }

    fn check_unset(&self) -> Result<(), String> {
        if self.backend_type == PLACEHOLDER
            || self.check_interval == PLACEHOLDER
            || self.cluster_policy == PLACEHOLDER
            || self.log_config == PLACEHOLDER
            || self.users_config == PLACEHOLDER
            || self.name == PLACEHOLDER
            || self.operation_timeout == PLACEHOLDER
        {
            let msg = "some of the fields present, but empty".to_string();
            error!("{}", msg);
            Err(msg)
        } else {
            Ok(())
        }
    }

    fn default_init_par_degree() -> usize {
        1
    }

    pub fn get_from_string(file: &str, cluster: &ClusterConfig) -> Result<NodeConfig, String> {
        let config = YamlBobConfig::parse::<NodeConfig>(file)?;
        debug!("config: {:?}", config);
        if let Err(e) = config.validate() {
            Err(format!("config is not valid: {}", e))
        } else {
            cluster.check(&config)?;
            debug!("cluster config is valid");
            Ok(config)
        }
    }

    fn default_disk_access_par_degree() -> usize {
        1
    }

    fn default_http_api_port() -> u16 {
        8000
    }

    fn default_http_api_address() -> IpAddr {
        IpAddr::V4(Ipv4Addr::UNSPECIFIED)
    }

    pub fn default_holder_group_size() -> usize {
        8
    }
}

impl Validatable for NodeConfig {
    fn validate(&self) -> Result<(), String> {
        self.check_unset()?;
        if self.backend_result().is_ok() && self.backend_type() == BackendType::Pearl {
            if let Some(pearl) = &self.pearl {
                pearl.validate()?;
            } else {
                let msg = "selected pearl backend, but pearl config not set".to_string();
                error!("{}", msg);
                return Err(msg);
            }
        }
        self.operation_timeout
            .parse::<HumanDuration>()
            .map_err(|e| {
                let msg = "field \'timeout\' for \'config\' is not valid".to_string();
                error!("{}, {}", msg, e);
                msg
            })?;
        self.check_interval.parse::<HumanDuration>().map_err(|e| {
            let msg = "field \'check_interval\' for \'config\' is not valid".to_string();
            error!("{}, {}", msg, e);
            msg
        })?;
        if self.name.is_empty() {
            let msg = "field \'name\' for \'config\' is empty".to_string();
            error!("{}", msg);
            Err(msg)
        } else if self.cluster_policy.is_empty() {
            let msg = "field \'cluster_policy\' for \'config\' is empty".to_string();
            error!("{}", msg);
            Err(msg)
        } else if self.users_config.is_empty() {
            let msg = "field \'users_config\' for \'config\' is empty".to_string();
            error!("{}", msg);
            Err(msg)
        } else if self.log_config.is_empty() {
            let msg = "field \'log_config\' for \'config\' is empty".to_string();
            error!("{}", msg);
            Err(msg)
        } else if self.quorum == 0 {
            let msg = "field \'quorum\' for \'config\' must be greater than 0".to_string();
            error!("{}", msg);
            Err(msg)
        } else {
            self.metrics
                .as_ref()
                .map_or(Ok(()), |metrics| metrics.validate())
        }
    }
}

pub mod tests {
    use crate::configs::node::Node as NodeConfig;
    use bob_access::AuthenticationType;

    use std::sync::Arc;

    pub fn node_config(name: &str, quorum: usize) -> NodeConfig {
        NodeConfig {
            log_config: "".to_string(),
            users_config: "".to_string(),
            name: name.to_string(),
            quorum,
            operation_timeout: "3sec".to_string(),
            check_interval: "3sec".to_string(),
            cluster_policy: "quorum".to_string(),
            backend_type: "in_memory".to_string(),
            pearl: None,
            metrics: None,
            bind_ref: Arc::default(),
            disks_ref: Arc::default(),
            cleanup_interval: "1d".to_string(),
            open_blobs_soft_limit: None,
            open_blobs_hard_limit: None,
            init_par_degree: 1,
            disk_access_par_degree: 1,
            count_interval: "10000ms".to_string(),
            http_api_port: NodeConfig::default_http_api_port(),
            http_api_address: NodeConfig::default_http_api_address(),
            bind_to_ip_address: None,
            bloom_filter_memory_limit: None,
            index_memory_limit: None,
            index_memory_limit_soft: None,
            holder_group_size: 8,
            authentication_type: AuthenticationType::None,
            hostname_resolve_dur_ms: NodeConfig::default_hostname_resolve_dur_ms(),
        }
    }
}
