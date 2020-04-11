use super::prelude::*;

const PLACEHOLDER: &str = "~";

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub(crate) struct BackendSettings {
    root_dir_name: String,
    alien_root_dir_name: String,
    timestamp_period: String,
    create_pearl_wait_delay: String,
}

impl Validatable for BackendSettings {
    fn validate(&self) -> Result<(), String> {
        self.check_unset()?;
        if self.root_dir_name.is_empty() {
            let msg = format!("field 'root_dir_name' for backend settings config is empty");
            error!("{}", msg);
            return Err(msg);
        }

        if self.alien_root_dir_name.is_empty() {
            let msg = format!("field 'alien_root_dir_name' for 'backend settings config' is empty");
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
        } else {
            let period = chrono::Duration::from_std(self.timestamp_period())
                .expect("smth wrong with time: {:?}, error: {}");
            if period > chrono::Duration::weeks(1) {
                let msg = format!(
                    "field 'timestamp_period' for 'backend settings config' is greater then week"
                );
                error!("{}", msg);
                return Err(msg);
            }
        };
        if self
            .create_pearl_wait_delay
            .parse::<HumanDuration>()
            .is_err()
        {
            let msg =
                format!("field 'create_pearl_wait_delay' for backend settings config is not valid");
            error!("{}", msg);
            Err(msg)
        } else {
            Ok(())
        }
    }
}

impl BackendSettings {
    pub(crate) fn root_dir_name(&self) -> &str {
        &self.root_dir_name
    }

    pub(crate) fn alien_root_dir_name(&self) -> &str {
        &self.alien_root_dir_name
    }

    pub(crate) fn timestamp_period(&self) -> Duration {
        self.timestamp_period
            .parse::<HumanDuration>()
            .expect("parse humantime duration")
            .into()
    }

    pub(crate) fn create_pearl_wait_delay(&self) -> Duration {
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
            let msg = format!("some of the fields present, but empty");
            error!("{}", msg);
            Err(msg)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub(crate) struct MetricsConfig {
    name: String,
    graphite: String,
}

impl MetricsConfig {
    pub(crate) fn graphite(&self) -> &str {
        &self.graphite
    }

    fn check_unset(&self) -> Result<(), String> {
        if self.name == PLACEHOLDER || self.graphite == PLACEHOLDER {
            let msg = format!("some of the fields present, but empty");
            error!("{}", msg);
            Err(msg)
        } else {
            Ok(())
        }
    }
}

impl Validatable for MetricsConfig {
    fn validate(&self) -> Result<(), String> {
        self.check_unset()?;
        if self.name.is_empty() {
            debug!("field 'name' for 'metrics config' is empty");
            return Err("field 'name' for 'metrics config' is empty".to_string());
        }

        if let Err(e) = self.graphite.parse::<SocketAddr>() {
            let msg = format!("field 'graphite': {} for 'metrics config' is invalid", e);
            error!("{}", msg);
            Err(msg)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub(crate) struct Pearl {
    #[serde(default = "Pearl::default_max_blob_size")]
    max_blob_size: u64,
    #[serde(default = "Pearl::default_max_data_in_blob")]
    max_data_in_blob: u64,
    #[serde(default = "Pearl::default_blob_file_name_prefix")]
    blob_file_name_prefix: String,
    #[serde(default = "Pearl::default_fail_retry_timeout")]
    fail_retry_timeout: String,
    alien_disk: String,
    #[serde(default = "Pearl::default_allow_duplicates")]
    allow_duplicates: bool,
    settings: BackendSettings,
}

impl Pearl {
    pub(crate) fn alien_disk(&self) -> &str {
        &self.alien_disk
    }

    fn default_fail_retry_timeout() -> String {
        "100ms".to_string()
    }

    pub(crate) fn fail_retry_timeout(&self) -> Duration {
        self.fail_retry_timeout
            .parse::<HumanDuration>()
            .expect("parse humantime duration")
            .into()
    }

    pub(crate) fn settings(&self) -> &BackendSettings {
        &self.settings
    }

    fn prepare(&self) {
        self.fail_retry_timeout(); // TODO check unwrap
    }

    fn default_allow_duplicates() -> bool {
        true
    }

    pub(crate) fn allow_duplicates(&self) -> bool {
        self.allow_duplicates
    }

    fn default_blob_file_name_prefix() -> String {
        "bob".to_string()
    }

    pub(crate) fn blob_file_name_prefix(&self) -> &str {
        &self.blob_file_name_prefix
    }

    fn default_max_data_in_blob() -> u64 {
        1_000_000
    }

    pub(crate) fn max_data_in_blob(&self) -> u64 {
        self.max_data_in_blob
    }

    fn default_max_blob_size() -> u64 {
        1_000_000
    }

    pub(crate) fn max_blob_size(&self) -> u64 {
        self.max_blob_size
    }

    fn check_unset(&self) -> Result<(), String> {
        if self.alien_disk == PLACEHOLDER
            || self.blob_file_name_prefix == PLACEHOLDER
            || self.fail_retry_timeout == PLACEHOLDER
        {
            let msg = format!("some of the fields present, but empty");
            error!("{}", msg);
            Err(msg)
        } else {
            Ok(())
        }
    }
}

impl Validatable for Pearl {
    fn validate(&self) -> Result<(), String> {
        self.check_unset()?;
        if self.alien_disk.is_empty() {
            debug!("field 'alien_disk' for 'config' is empty");
            return Err("field 'alien_disk' for 'config' is empty".to_string());
        }

        if self.fail_retry_timeout.parse::<HumanDuration>().is_err() {
            debug!("field 'fail_retry_timeout' for 'config' is not valid");
            Err("field 'fail_retry_timeout' for 'config' is not valid".to_string())
        } else {
            self.settings.validate()
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Copy)]
pub(crate) enum BackendType {
    InMemory = 0,
    Stub,
    Pearl,
}

/// Node configuration struct, stored in node.yaml.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct NodeConfig {
    log_config: String,
    name: String,
    quorum: usize,
    operation_timeout: String,
    check_interval: String,
    cluster_policy: String,

    backend_type: String,
    pearl: Option<Pearl>,
    metrics: Option<MetricsConfig>,

    #[serde(skip)]
    bind_ref: RefCell<String>,
    #[serde(skip)]
    disks_ref: RefCell<Vec<DiskPath>>,
}

impl NodeConfig {
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn quorum(&self) -> usize {
        self.quorum
    }

    pub(crate) fn pearl(&self) -> &Pearl {
        self.pearl.as_ref().expect("get pearl config")
    }

    pub(crate) fn metrics(&self) -> &MetricsConfig {
        self.metrics.as_ref().expect("metrics config")
    }

    /// Get log config file path.
    pub fn log_config(&self) -> &str {
        &self.log_config
    }

    pub(crate) fn cluster_policy(&self) -> &str {
        &self.cluster_policy
    }

    /// Get reference to bind address.
    pub fn bind(&self) -> Ref<String> {
        self.bind_ref.borrow()
    }

    /// Get grpc request operation timeout, parsed from humantime format.
    pub fn operation_timeout(&self) -> Duration {
        self.operation_timeout
            .parse::<HumanDuration>()
            .expect("parse humantime duration")
            .into()
    }

    pub(crate) fn check_interval(&self) -> Duration {
        self.check_interval
            .parse::<HumanDuration>()
            .expect("parse humantime duration")
            .into()
    }

    pub(crate) fn disks(&self) -> Vec<DiskPath> {
        self.disks_ref.borrow().clone()
    }

    pub(crate) fn backend_type(&self) -> BackendType {
        self.backend_result().expect("clone backend type")
    }

    pub(crate) fn backend_result(&self) -> Result<BackendType, String> {
        match self.backend_type.as_str() {
            "in_memory" => Ok(BackendType::InMemory),
            "stub" => Ok(BackendType::Stub),
            "pearl" => Ok(BackendType::Pearl),
            value => Err(format!("unknown backend type: {}", value)),
        }
    }

    pub(crate) fn prepare(&self, node: &Node) -> Result<(), String> {
        self.bind_ref.replace(node.address().to_owned());

        let t = node
            .disks()
            .iter()
            .map(|disk| DiskPath::new(disk.name().to_owned(), disk.path().to_owned()))
            .collect::<Vec<_>>();
        self.disks_ref.replace(t);

        self.backend_result()?;

        if self.backend_type() == BackendType::Pearl {
            if let Some(pearl) = &self.pearl {
                pearl.prepare();
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn get_from_string(
        file: &str,
        cluster: &ClusterConfig,
    ) -> Result<NodeConfig, String> {
        let config = YamlBobConfigReader::parse::<NodeConfig>(file)?;
        debug!("config: {:?}", config);
        if let Err(e) = config.validate() {
            debug!("config is not valid: {}", e);
            Err(format!("config is not valid: {}", e))
        } else {
            debug!("config is valid");
            cluster.check(&config)?;
            debug!("cluster config is valid");
            Ok(config)
        }
    }

    fn check_unset(&self) -> Result<(), String> {
        if self.backend_type == PLACEHOLDER
            || self.check_interval == PLACEHOLDER
            || self.cluster_policy == PLACEHOLDER
            || self.log_config == PLACEHOLDER
            || self.name == PLACEHOLDER
            || self.operation_timeout == PLACEHOLDER
        {
            let msg = format!("some of the fields present, but empty");
            error!("{}", msg);
            Err(msg)
        } else {
            Ok(())
        }
    }
}

impl Validatable for NodeConfig {
    fn validate(&self) -> Result<(), String> {
        self.check_unset()?;
        if self.backend_result().is_ok() && self.backend_type() == BackendType::Pearl {
            if let Some(pearl) = &self.pearl {
                pearl.validate()?;
            } else {
                let msg = format!("selected pearl backend, but pearl config not set");
                error!("{}", msg);
                return Err(msg);
            }
        }
        self.operation_timeout
            .parse::<HumanDuration>()
            .map_err(|_| {
                debug!("field 'timeout' for 'config' is not valid");
                "field 'timeout' for 'config' is not valid".to_string()
            })?;
        self.check_interval.parse::<HumanDuration>().map_err(|_| {
            debug!("field 'check_interval' for 'config' is not valid");
            "field 'check_interval' for 'config' is not valid".to_string()
        })?;
        if self.name.is_empty() {
            debug!("field 'name' for 'config' is empty");
            return Err("field 'name' for 'config' is empty".to_string());
        }
        if self.cluster_policy.is_empty() {
            debug!("field 'cluster_policy' for 'config' is empty");
            return Err("field 'cluster_policy' for 'config' is empty".to_string());
        }
        if self.log_config.is_empty() {
            debug!("field 'log_config' for 'config' is empty");
            return Err("field 'log_config' for 'config' is empty".to_string());
        }
        if self.quorum == 0 {
            debug!("field 'quorum' for 'config' must be greater than 0");
            return Err("field 'quorum' for 'config' must be greater than 0".to_string());
        }
        if let Some(metrics) = &self.metrics {
            metrics.validate()
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    pub(crate) fn node_config(name: &str, quorum: usize) -> NodeConfig {
        NodeConfig {
            log_config: "".to_string(),
            name: name.to_string(),
            quorum: quorum,
            operation_timeout: "3sec".to_string(),
            check_interval: "3sec".to_string(),
            cluster_policy: "quorum".to_string(),
            backend_type: "in_memory".to_string(),
            pearl: None,
            metrics: None,
            bind_ref: RefCell::default(),
            disks_ref: RefCell::default(),
        }
    }
}
