use crate::core::configs::cluster::{Cluster, Node};
use crate::core::configs::reader::{BobConfigReader, Validatable, YamlBobConfigReader};
use log::LevelFilter;
use std::cell::Cell;
use std::cell::RefCell;
use std::time::Duration;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum LogLevel {
    Off = 0,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeConfig {
    pub log_level: Option<LogLevel>,
    pub name: Option<String>,
    pub quorum: Option<u8>,
    pub timeout: Option<String>,
    pub check_interval: Option<String>,

    #[serde(skip)]
    pub bind_ref: RefCell<String>,
    #[serde(skip)]
    pub timeout_ref: Cell<Duration>,
    #[serde(skip)]
    pub check_ref: Cell<Duration>,
}

impl NodeConfig {
    pub fn bind(&self) -> String {
        self.bind_ref.borrow().to_string()
    }
    pub fn timeout(&self) -> Duration {
        self.timeout_ref.get()
    }
    pub fn check_interval(&self) -> Duration {
        self.check_ref.get()
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

        Ok(())
    }
}
impl Validatable for NodeConfig {
    fn validate(&self) -> Option<String> {
        if self.timeout.is_none() {
            debug!("field 'timeout' for 'config' is not set");
            return Some("field 'timeout' for 'config' is not set".to_string());
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
