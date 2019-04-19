use crate::core::configs::cluster::Cluster;
use crate::core::configs::reader::{BobConfigReader, Validatable, YamlBobConfigReader};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum LogLevel {
    Debug,
    Error,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeConfig {
    pub log_level: Option<LogLevel>,
    pub name: Option<String>,
}

impl Validatable for NodeConfig {
    fn validate(&self) -> Option<String> {
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
