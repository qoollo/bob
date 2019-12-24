use super::prelude::*;

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct BackendSettings {
    pub root_dir_name: Option<String>,
    pub alien_root_dir_name: Option<String>,
    pub timestamp_period: Option<String>,
    pub create_pearl_wait_delay: Option<String>,
}

impl Validatable for BackendSettings {
    fn validate(&self) -> Result<(), String> {
        if let Some(root_dir_name) = &self.root_dir_name {
            if root_dir_name.is_empty() {
                debug!("field 'root_dir_name' for 'backend settings config' is empty");
                return Err(
                    "field 'root_dir_name' for 'backend settings config' is empty".to_string(),
                );
            }
        }
        if let Some(alien_root_dir_name) = &self.alien_root_dir_name {
            if alien_root_dir_name.is_empty() {
                debug!("field 'alien_root_dir_name' for 'backend settings config' is empty");
                return Err(
                    "field 'alien_root_dir_name' for 'backend settings config' is empty"
                        .to_string(),
                );
            }
        }
        match &self.timestamp_period {
            None => {
                debug!("field 'timestamp_period' for 'backend settings config' is not set");
                return Err(
                    "field 'timestamp_period' for 'backend settings config' is not set".to_string(),
                );
            }
            Some(period) => {
                if period.parse::<HumanDuration>().is_err() {
                    debug!("field 'timestamp_period' for 'backend settings config' is not valid");
                    return Err(
                        "field 'timestamp_period' for 'backend settings config' is not valid"
                            .to_string(),
                    );
                } else {
                    let period: chrono::Duration =
                        chrono::Duration::from_std(self.timestamp_period()).map_err(|e| {
                            trace!("smth wrong with time: {:?}, error: {}", period, e);
                            format!("smth wrong with time: {:?}, error: {}", period, e)
                        })?;
                    if period > chrono::Duration::weeks(1) {
                        return Err("field 'timestamp_period' for 'backend settings config' is greater then week".to_string());
                    }
                }
            }
        };
        match &self.create_pearl_wait_delay {
            None => {
                debug!("field 'create_pearl_wait_delay' for 'backend settings config' is not set");
                return Err(
                    "field 'create_pearl_wait_delay' for 'backend settings config' is not set"
                        .to_string(),
                );
            }
            Some(timeout) => {
                if timeout.parse::<HumanDuration>().is_err() {
                    debug!("field 'create_pearl_wait_delay' for 'backend settings config' is not valid");
                    return Err("field 'create_pearl_wait_delay' for 'backend settings config' is not valid".to_string());
                }
            }
        };
        Ok(())
    }
}

impl BackendSettings {
    pub fn root_dir_name(&self) -> String {
        self.root_dir_name.clone().expect("clone root dir name")
    }

    pub fn alien_root_dir_name(&self) -> String {
        self.alien_root_dir_name
            .clone()
            .expect("clone alien root dir name")
    }

    pub fn timestamp_period(&self) -> Duration {
        self.timestamp_period
            .clone()
            .expect("clone timestamp period")
            .parse::<HumanDuration>()
            .expect("parse humantime duration")
            .into()
    }

    pub fn create_pearl_wait_delay(&self) -> Duration {
        self.create_pearl_wait_delay
            .clone()
            .expect("clone create pearl wait delay")
            .parse::<HumanDuration>()
            .expect("parse humantime duration")
            .into()
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct MetricsConfig {
    pub name: Option<String>,
    pub graphite: Option<String>,
}

impl Validatable for MetricsConfig {
    fn validate(&self) -> Result<(), String> {
        if let Some(name) = &self.name {
            if name.is_empty() {
                debug!("field 'name' for 'metrics config' is empty");
                return Err("field 'name' for 'metrics config' is empty".to_string());
            }
        }

        if let Some(value) = self.graphite.as_ref() {
            value.parse::<SocketAddr>().map_err(|_| {
                debug!(
                    "field 'graphite': {} for 'metrics config' is invalid",
                    value
                );
                format!(
                    "field 'graphite': {} for 'metrics config' is invalid",
                    value
                )
            })?;
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct PearlConfig {
    pub max_blob_size: Option<u64>,
    pub max_data_in_blob: Option<u64>,
    pub blob_file_name_prefix: Option<String>,
    pub fail_retry_timeout: Option<String>,
    pub alien_disk: Option<String>,
    pub allow_duplicates: Option<bool>,

    pub settings: Option<BackendSettings>,
}

impl PearlConfig {
    pub fn alien_disk(&self) -> String {
        self.alien_disk.clone().expect("clone alien disk")
    }

    pub fn fail_retry_timeout(&self) -> Duration {
        self.fail_retry_timeout
            .clone()
            .expect("clone fail retry timeout")
            .parse::<HumanDuration>()
            .expect("parse humantime duration")
            .into()
    }

    pub fn settings(&self) -> BackendSettings {
        self.settings.clone().expect("clone settings")
    }

    pub fn prepare(&self) {
        self.fail_retry_timeout(); // TODO check unwrap
    }
}

impl Validatable for PearlConfig {
    fn validate(&self) -> Result<(), String> {
        self.max_blob_size.ok_or_else(|| {
            debug!("field 'max_blob_size' for 'config' is not set");
            "field 'max_blob_size' for 'config' is not set".to_string()
        })?;
        match &self.alien_disk {
            None => {
                debug!("field 'alien_disk' for 'config' is not set");
                return Err("field 'alien_disk' for 'config' is not set".to_string());
            }
            Some(alien_disk) => {
                if alien_disk.is_empty() {
                    debug!("field 'alien_disk' for 'config' is empty");
                    return Err("field 'alien_disk' for 'config' is empty".to_string());
                }
            }
        };
        match &self.fail_retry_timeout {
            None => {
                debug!("field 'fail_retry_timeout' for 'config' is not set");
                return Err("field 'fail_retry_timeout' for 'config' is not set".to_string());
            }
            Some(timeout) => {
                if timeout.parse::<HumanDuration>().is_err() {
                    debug!("field 'fail_retry_timeout' for 'config' is not valid");
                    return Err("field 'fail_retry_timeout' for 'config' is not valid".to_string());
                }
            }
        };

        match &self.settings {
            None => {
                debug!("field 'settings' for 'config' is not set");
                Err("field 'settings' for 'config' is not set".to_string())
            }
            Some(settings) => settings.validate(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct DiskPath {
    pub name: String,
    pub path: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Copy)]
pub enum BackendType {
    InMemory = 0,
    Stub,
    Pearl,
}

#[derive(Debug, PartialEq, Deserialize)]
pub struct NodeConfig {
    pub log_config: Option<String>,
    pub name: Option<String>,
    pub quorum: Option<u8>,
    pub operation_timeout: Option<String>,
    pub check_interval: Option<String>,
    pub cluster_policy: Option<String>,

    pub backend_type: Option<String>,
    pub pearl: Option<PearlConfig>,
    pub metrics: Option<MetricsConfig>,

    #[serde(skip)]
    pub bind_ref: RefCell<String>,
    #[serde(skip)]
    pub disks_ref: RefCell<Vec<DiskPath>>,
}

impl NodeConfig {
    pub fn name(&self) -> String {
        self.name.clone().expect("config name")
    }

    pub fn pearl(&self) -> PearlConfig {
        self.pearl.clone().expect("config pearl")
    }
    pub fn log_config(&self) -> String {
        self.log_config.clone().expect("config log config")
    }
    pub fn cluster_policy(&self) -> String {
        self.cluster_policy.clone().expect("config cluster policy")
    }
    pub fn bind(&self) -> String {
        self.bind_ref.borrow().to_string()
    }
    pub fn operation_timeout(&self) -> Duration {
        self.operation_timeout
            .as_ref()
            .expect("get config operation timeout")
            .parse::<HumanDuration>()
            .expect("parse humantime duration")
            .into()
    }
    pub fn check_interval(&self) -> Duration {
        self.check_interval
            .as_ref()
            .expect("get config check interval")
            .parse::<HumanDuration>()
            .expect("parse humantime duration")
            .into()
    }
    pub fn disks(&self) -> Vec<DiskPath> {
        self.disks_ref.borrow().clone()
    }
    pub fn backend_type(&self) -> BackendType {
        self.backend_result().expect("clone backend type")
    }
    fn backend_result(&self) -> Result<BackendType, String> {
        match self
            .backend_type
            .as_ref()
            .expect("match backend type")
            .as_str()
        {
            "in_memory" => Ok(BackendType::InMemory),
            "stub" => Ok(BackendType::Stub),
            "pearl" => Ok(BackendType::Pearl),
            value => Err(format!("unknown backend type: {}", value)),
        }
    }
    pub fn prepare(&self, node: &Node) -> Result<(), String> {
        self.bind_ref
            .replace(node.address.clone().expect("clone node address"));

        self.disks_ref.replace(
            node.disks
                .iter()
                .map(|disk| DiskPath {
                    name: disk.name.clone().expect("clone disk name"),
                    path: disk.path.clone().expect("clone disk path"),
                })
                .collect::<Vec<_>>(),
        );

        self.backend_result()?;

        if self.backend_type() == BackendType::Pearl {
            self.pearl.as_ref().expect("prepare pearl").prepare();
        }
        Ok(())
    }
}
impl Validatable for NodeConfig {
    fn validate(&self) -> Result<(), String> {
        match &self.backend_type {
            None => {
                debug!("field 'backend_type' for 'config' is not set");
                return Err("field 'backend_type' for 'config' is not set".to_string());
            }
            Some(_) => {
                if self.backend_result().is_ok() && self.backend_type() == BackendType::Pearl {
                    match &self.pearl {
                        None => {
                            debug!("choosed 'Pearl' value for field 'backend_type' but 'pearl' config is not set");
                            Err("choosed 'Pearl' value for field 'backend_type' but 'pearl' config is not set".to_string())
                        }
                        Some(pearl) => pearl.validate(),
                    }?;
                };
            }
        };
        match &self.operation_timeout {
            None => {
                debug!("field 'timeout' for 'config' is not set");
                return Err("field 'timeout' for 'config' is not set".to_string());
            }
            Some(timeout) => {
                timeout.parse::<HumanDuration>().map_err(|_| {
                    debug!("field 'timeout' for 'config' is not valid");
                    "field 'timeout' for 'config' is not valid".to_string()
                })?;
            }
        };
        match &self.check_interval {
            None => {
                debug!("field 'check_interval' for 'config' is not set");
                return Err("field 'check_interval' for 'config' is not set".to_string());
            }
            Some(check_interval) => {
                check_interval.parse::<HumanDuration>().map_err(|_| {
                    debug!("field 'check_interval' for 'config' is not valid");
                    "field 'check_interval' for 'config' is not valid".to_string()
                })?;
            }
        };
        match &self.name {
            None => {
                debug!("field 'name' for 'config' is not set");
                return Err("field 'name' for 'config' is not set".to_string());
            }
            Some(name) => {
                if name.is_empty() {
                    debug!("field 'name' for 'config' is empty");
                    return Err("field 'name' for 'config' is empty".to_string());
                }
            }
        };
        match &self.cluster_policy {
            None => {
                debug!("field 'cluster_policy' for 'config' is not set");
                return Err("field 'cluster_policy' for 'config' is not set".to_string());
            }
            Some(cluster_policy) => {
                if cluster_policy.is_empty() {
                    debug!("field 'cluster_policy' for 'config' is empty");
                    return Err("field 'cluster_policy' for 'config' is empty".to_string());
                }
            }
        };
        match &self.log_config {
            None => {
                debug!("field 'log_config' for 'config' is not set");
                return Err("field 'log_config' for 'config' is not set".to_string());
            }
            Some(log_config) => {
                if log_config.is_empty() {
                    debug!("field 'log_config' for 'config' is empty");
                    return Err("field 'log_config' for 'config' is empty".to_string());
                }
            }
        };
        match self.quorum {
            None => {
                debug!("field 'quorum' for 'config' is not set");
                return Err("field 'quorum' for 'config' is not set".to_string());
            }
            Some(quorum) => {
                if quorum == 0 {
                    debug!("field 'quorum' for 'config' must be greater than 0");
                    return Err("field 'quorum' for 'config' must be greater than 0".to_string());
                }
            }
        };
        if let Some(metrics) = &self.metrics {
            metrics.validate()
        } else {
            Ok(())
        }
    }
}

pub struct NodeConfigYaml {}

impl NodeConfigYaml {
    pub fn check(cluster: &ClusterConfig, node: &NodeConfig) -> Result<(), String> {
        let finded = cluster
            .nodes
            .iter()
            .find(|n| n.name == node.name)
            .ok_or_else(|| {
                debug!("cannot find node: {} in cluster config", node.name());
                format!("cannot find node: {} in cluster config", node.name())
            })?;
        if node.backend_result().is_ok() && node.backend_type() == BackendType::Pearl {
            let pearl = node.pearl.as_ref().expect("get node pearl");
            finded
                .disks
                .iter()
                .find(|d| d.name == pearl.alien_disk)
                .ok_or_else(|| {
                    debug!(
                        "cannot find disk {:?} for node {:?} in cluster config",
                        pearl.alien_disk, node.name
                    );
                    format!(
                        "cannot find disk {:?} for node {:?} in cluster config",
                        pearl.alien_disk, node.name
                    )
                })?;
        }
        node.prepare(finded)
    }

    pub fn check_cluster(cluster: &ClusterConfig, node: &NodeConfig) -> Result<(), String> {
        Self::check(cluster, node) //TODO
    }

    pub fn get(filename: &str, cluster: &ClusterConfig) -> Result<NodeConfig, String> {
        let config = YamlBobConfigReader::get::<NodeConfig>(filename)?;

        match config.validate() {
            Ok(_) => {
                Self::check_cluster(cluster, &config)?;
                Ok(config)
            }
            Err(e) => {
                debug!("config is not valid: {}", e);
                Err(format!("config is not valid: {}", e))
            }
        }
    }

    pub fn get_from_string(file: &str, cluster: &ClusterConfig) -> Result<NodeConfig, String> {
        let config = YamlBobConfigReader::parse::<NodeConfig>(file)?;
        debug!("config: {:?}", config);
        if let Err(e) = config.validate() {
            debug!("config is not valid: {}", e);
            Err(format!("config is not valid: {}", e))
        } else {
            debug!("config is valid");
            Self::check_cluster(cluster, &config)?;
            debug!("cluster config is valid");
            Ok(config)
        }
    }
}

pub mod tests {
    use super::*;
    pub fn node_config(name: &str, quorum: u8) -> NodeConfig {
        NodeConfig {
            log_config: Some("".to_string()),
            name: Some(name.to_string()),
            quorum: Some(quorum),
            operation_timeout: Some("3sec".to_string()),
            check_interval: Some("3sec".to_string()),
            cluster_policy: Some("quorum".to_string()),
            backend_type: Some("in_memory".to_string()),
            pearl: None,
            metrics: None,
            bind_ref: RefCell::default(),
            disks_ref: RefCell::default(),
        }
    }
}
