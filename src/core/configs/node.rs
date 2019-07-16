use crate::core::configs::{
    cluster::{ClusterConfig, Node},
    reader::{Validatable, YamlBobConfigReader},
};
use log::LevelFilter;
use std::{
    cell::{Cell, RefCell},
    time::Duration,
};

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct PearlConfig {
    pub max_blob_size: Option<u64>,
    pub max_data_in_blob: Option<u64>,
    pub blob_file_name_prefix: Option<String>,
    pub pool_count_threads: Option<u16>,
    pub fail_retry_timeout: Option<String>,
    pub alien_disk: Option<String>,
}

impl PearlConfig {
    pub fn pool_count_threads(&self) -> u16 {
        self.pool_count_threads.unwrap()
    }
    pub fn alien_disk(&self) -> String {
        self.alien_disk.as_ref().unwrap().clone()
    }
    pub fn fail_retry_timeout(&self) -> Duration {
        let t: Duration = self
            .fail_retry_timeout
            .as_ref()
            .unwrap()
            .clone()
            .parse::<humantime::Duration>()
            .unwrap()
            .into();
        t
    }

    pub fn prepare(&self) -> Result<(), String> {
        let _ = self.fail_retry_timeout(); // TODO check unwrap

        Ok(())
    }
}

impl Validatable for PearlConfig {
    fn validate(&self) -> Result<(), String> {
        match self.max_blob_size {
            None => {
                debug!("field 'max_blob_size' for 'config' is not set");
                return Err("field 'max_blob_size' for 'config' is not set".to_string());
            }
            _ => {}
        };
        match self.pool_count_threads {
            None => {
                debug!("field 'pool_count_threads' for 'config' is not set");
                return Err("field 'pool_count_threads' for 'config' is not set".to_string());
            }
            _ => {}
        };
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
                if timeout.parse::<humantime::Duration>().is_err() {
                    debug!("field 'fail_retry_timeout' for 'config' is not valid");
                    return Err("field 'fail_retry_timeout' for 'config' is not valid".to_string());
                }
            }
        };
        Ok(())
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
    pub ping_threads_count: Option<u8>,

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
    pub fn ping_threads_count(&self) -> u8 {
        self.ping_threads_count.unwrap()
    }
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
        match self.backend_type.as_ref().unwrap().as_str() {
            "in_memory" => Ok(BackendType::InMemory),
            "stub" => Ok(BackendType::Stub),
            "pearl" => Ok(BackendType::Pearl),
            value => Err(format!("unknown backend type: {}", value)),
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

        if self.backend_type() == BackendType::Pearl {
            match &self.pearl {
                Some(pearl) => pearl.prepare()?,
                _ => panic!("cannot match pearl"),
            };
        };
        Ok(())
    }
}
impl Validatable for NodeConfig {
    fn validate(&self) -> Result<(), String> {
        match self.ping_threads_count {
            None => {
                debug!("field 'ping_threads_count' for 'config' is not set");
                return Err("field 'ping_threads_count' for 'config' is not set".to_string());
            }
            _ => {}
        };
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
                            return Err("choosed 'Pearl' value for field 'backend_type' but 'pearl' config is not set".to_string());
                        }
                        Some(pearl) => pearl.validate()?,
                    };
                };
            }
        };
        match &self.timeout {
            None => {
                debug!("field 'timeout' for 'config' is not set");
                return Err("field 'timeout' for 'config' is not set".to_string());
            }
            Some(timeout) => {
                if timeout.parse::<humantime::Duration>().is_err() {
                    debug!("field 'timeout' for 'config' is not valid");
                    return Err("field 'timeout' for 'config' is not valid".to_string());
                }
            }
        };
        match &self.check_interval {
            None => {
                debug!("field 'check_interval' for 'config' is not set");
                return Err("field 'check_interval' for 'config' is not set".to_string());
            }
            Some(check_interval) => {
                if check_interval.parse::<humantime::Duration>().is_err() {
                    debug!("field 'check_interval' for 'config' is not valid");
                    return Err("field 'check_interval' for 'config' is not valid".to_string());
                }
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
        match self.log_level {
            None => {
                debug!("field 'log_level' for 'config' is not set");
                return Err("field 'log_level' for 'config' is not set".to_string());
            }
            _ => {}
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

        Ok(())
    }
}

pub struct NodeConfigYaml {}

impl NodeConfigYaml {
    pub fn check_cluster(&self, cluster: &ClusterConfig, node: &NodeConfig) -> Result<(), String> {
        let finded = cluster.nodes.iter().find(|n| n.name == node.name);
        if finded.is_none() {
            debug!("cannot find node: {} in cluster config", node.name());
            return Err(format!(
                "cannot find node: {} in cluster config",
                node.name()
            ));
        }
        if node.backend_result().is_ok() && node.backend_type() == BackendType::Pearl {
            let pearl = node.pearl.as_ref().unwrap();
            let finded_disk = finded
                .unwrap()
                .disks
                .iter()
                .find(|d| d.name == pearl.alien_disk);
            if finded_disk.is_none() {
                debug!(
                    "cannot find disk {:?} for node {:?} in cluster config",
                    pearl.alien_disk, node.name
                );
                return Err(format!(
                    "cannot find disk {:?} for node {:?} in cluster config",
                    pearl.alien_disk, node.name
                ));
            }
        }
        node.prepare(finded.unwrap())?;
        Ok(())
    }

    pub fn get(&self, filename: &str, cluster: &ClusterConfig) -> Result<NodeConfig, String> {
        let config: NodeConfig = YamlBobConfigReader {}.get::<NodeConfig>(filename)?;
        match config.validate() {
            Ok(_) => {
                self.check_cluster(cluster, &config)?;
                Ok(config)
            }
            Err(e) => {
                debug!("config is not valid: {}", e);
                Err(format!("config is not valid: {}", e))
            }
        }
    }

    pub fn get_from_string(
        &self,
        file: &str,
        cluster: &ClusterConfig,
    ) -> Result<NodeConfig, String> {
        let config: NodeConfig = YamlBobConfigReader {}.parse(file)?;
        match config.validate() {
            Ok(_) => {
                self.check_cluster(cluster, &config)?;
                Ok(config)
            }
            Err(e) => {
                debug!("config is not valid: {}", e);
                Err(format!("config is not valid: {}", e))
            }
        }
    }
}
