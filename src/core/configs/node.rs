use crate::core::configs::cluster::{Cluster, Node};
use crate::core::configs::reader::{BobConfigReader, Validatable, YamlBobConfigReader};
use log::LevelFilter;
use std::cell::Cell;
use std::cell::RefCell;
use std::time::Duration;

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct PearlConfig {
    pub max_blob_size: Option<u64>,
    pub max_data_in_blob: Option<u64>,
    pub blob_file_name_prefix: Option<String>,
    pub pool_count_threads: Option<u16>,
}

impl PearlConfig {
    pub fn pool_count_threads(&self) -> u16 {
        self.pool_count_threads.unwrap()
    }
}

impl Validatable for PearlConfig {
    fn validate(&self) -> Option<String> {
        if self.max_blob_size.is_none() {
            debug!("field 'max_blob_size' for 'pearl' is not set");
            return Some("field 'max_blob_size' for 'pearl' is not set".to_string());
        }
        if self.pool_count_threads.is_none() {
            debug!("field 'pool_count_threads' for 'pearl' is not set");
            return Some("field 'pool_count_threads' for 'pearl' is not set".to_string());
        }
        None
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum LogLevel {
    Off = 0,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
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

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeConfig {
    pub log_level: Option<LogLevel>,
    pub name: Option<String>,
    pub quorum: Option<u8>,
    pub timeout: Option<String>,
    pub check_interval: Option<String>,
    pub cluster_policy: Option<String>,

    pub backend_type: Option<String>,
    pub pearl: Option<PearlConfig>,

    #[serde(skip)]
    pub bind_ref: RefCell<String>,
    #[serde(skip)]
    pub timeout_ref: Cell<Duration>,
    #[serde(skip)]
    pub check_ref: Cell<Duration>,
    #[serde(skip)]
    pub disks_ref: RefCell<Vec<DiskPath>>,
}

impl NodeConfig {
    pub fn name(&self) -> String {
        self.name.as_ref().unwrap().clone()
    }
    pub fn cluster_policy(&self) -> String {
        self.cluster_policy.as_ref().unwrap().clone()
    }
    pub fn bind(&self) -> String {
        self.bind_ref.borrow().to_string()
    }
    pub fn timeout(&self) -> Duration {
        self.timeout_ref.get()
    }
    pub fn check_interval(&self) -> Duration {
        self.check_ref.get()
    }
    pub fn disks(&self) -> Vec<DiskPath> {
        self.disks_ref.borrow().clone()
    }
    pub fn log_level(&self) -> LevelFilter {
        match self.log_level {
            Some(LogLevel::Debug) => LevelFilter::Debug,
            Some(LogLevel::Error) => LevelFilter::Error,
            Some(LogLevel::Warn) => LevelFilter::Warn,
            Some(LogLevel::Info) => LevelFilter::Info,
            Some(LogLevel::Trace) => LevelFilter::Trace,
            Some(LogLevel::Off) => LevelFilter::Off,
            None => LevelFilter::Off,
        }
    }
    pub fn backend_type(&self) -> BackendType {
        self.backend_result().unwrap()
    }
    fn backend_result(&self) -> Result<BackendType, String> {
        let value = self.backend_type.as_ref().unwrap().clone();
        match value.as_ref() {
            "in_memory" => Ok(BackendType::InMemory),
            "stub" => Ok(BackendType::Stub),
            "pearl" => Ok(BackendType::Pearl),
            _ =>  Err(format!("unknown backend type: {}", value)),
        }
    }
    pub fn prepare(&self, node: &Node) -> Result<(), String> {
        self.bind_ref
            .replace(node.address.as_ref().unwrap().clone());

        let t: Duration = self
            .timeout
            .as_ref()
            .unwrap()
            .clone()
            .parse::<humantime::Duration>()
            .unwrap()
            .into();
        self.timeout_ref.set(t);

        let t1: Duration = self
            .check_interval
            .as_ref()
            .unwrap()
            .clone()
            .parse::<humantime::Duration>()
            .unwrap()
            .into();
        self.check_ref.set(t1);

        self.disks_ref.replace(
            node.disks
                .iter()
                .map(|disk| DiskPath {
                    name: disk.name.as_ref().unwrap().clone(),
                    path: disk.path.as_ref().unwrap().clone(),
                })
                .collect::<Vec<DiskPath>>(),
        );

        self.backend_result()?;
        Ok(())
    }
}
impl Validatable for NodeConfig {
    fn validate(&self) -> Option<String> {
        if self.timeout.is_none() {
            debug!("field 'timeout' for 'config' is not set");
            return Some("field 'timeout' for 'config' is not set".to_string());
        }
        if self.backend_type.is_none() {
            debug!("field 'backend_type' for 'config' is not set");
            return Some("field 'backend_type' for 'config' is not set".to_string());
        }
        if self.backend_result().ok()? == BackendType::Pearl {
            if self.pearl.is_none() {
                debug!("choosed 'Pearl' value for field 'backend_type' but 'pearl' config is not set");
                return Some("choosed 'Pearl' value for field 'backend_type' but 'pearl' config is not set".to_string());
            }
            let r = self.pearl.as_ref()?.validate();
            if r.is_some() {
                return r;
            }
        }
        if self
            .timeout
            .as_ref()?
            .clone()
            .parse::<humantime::Duration>()
            .is_err()
        {
            debug!("field 'timeout' for 'config' is not valid");
            return Some("field 'timeout' for 'config' is not valid".to_string());
        }
        if self.check_interval.is_none() {
            debug!("field 'check_interval' for 'config' is not set");
            return Some("field 'check_interval' for 'config' is not set".to_string());
        }
        if self
            .check_interval
            .as_ref()?
            .clone()
            .parse::<humantime::Duration>()
            .is_err()
        {
            debug!("field 'check_interval' for 'config' is not valid");
            return Some("field 'check_interval' for 'config' is not valid".to_string());
        }

        if self.name.is_none() {
            debug!("field 'name' for 'config' is not set");
            return Some("field 'name' for 'config' is not set".to_string());
        }
        if self.name.as_ref()?.is_empty() {
            debug!("field 'name' for 'config' is empty");
            return Some("field 'name' for 'config' is empty".to_string());
        }
        if self.cluster_policy.is_none() {
            debug!("field 'cluster_policy' for 'config' is not set");
            return Some("field 'cluster_policy' for 'config' is not set".to_string());
        }
        if self.cluster_policy.as_ref()?.is_empty() {
            debug!("field 'cluster_policy' for 'config' is empty");
            return Some("field 'cluster_policy' for 'config' is empty".to_string());
        }
        if self.log_level.is_none() {
            debug!("field 'log_level' for 'config' is not set");
            return Some("field 'log_level' for 'config' is not set".to_string());
        }
        if self.quorum.is_none() {
            debug!("field 'quorum' for 'config' is not set");
            return Some("field 'quorum' for 'config' is not set".to_string());
        }
        if self.quorum? == 0 {
            debug!("field 'quorum' for 'config' must be greater than 0");
            return Some("field 'quorum' for 'config' must be greater than 0".to_string());
        }
        None
    }
}

pub trait BobNodeConfig {
    fn check_cluster(&self, cluster: &Cluster, node: &NodeConfig) -> Result<(), String>;
    fn get(&self, filename: &str, cluster: &Cluster) -> Result<NodeConfig, String>;
}

pub struct NodeConfigYaml {}

impl BobNodeConfig for NodeConfigYaml {
    fn check_cluster(&self, cluster: &Cluster, node: &NodeConfig) -> Result<(), String> {
        let finded = cluster.nodes.iter().find(|n| n.name == node.name);
        if finded.is_none() {
            debug!(
                "cannot find node: {} in cluster config",
                node.name.as_ref().unwrap()
            );
            return Err(format!(
                "cannot find node: {} in cluster config",
                node.name.as_ref().unwrap()
            ));
        }
        node.prepare(finded.unwrap())?;
        Ok(())
    }

    fn get(&self, filename: &str, cluster: &Cluster) -> Result<NodeConfig, String> {
        let config: NodeConfig = YamlBobConfigReader {}.get(filename)?;
        let is_valid = config.validate();
        if is_valid.is_some() {
            debug!("config is not valid: {}", is_valid.as_ref().unwrap());
            return Err(format!("config is not valid: {}", is_valid.unwrap()));
        }
        self.check_cluster(cluster, &config)?;

        Ok(config)
    }
}
