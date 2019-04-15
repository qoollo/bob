use crate::core::configs::config::{Validatable, YamlBobConfigReader, BobConfigReader};

pub enum LogLevel {
    Debug,
    Error,
}

pub struct NodeConfig {
    pub log_level: Option<LogLevel>,
    pub name: Option<String>,
}

pub impl Validatable for NodeConfig {
    fn validate(&self) ->Some<String> {
         if self.name.is_none() {
            debug!("field 'name' for 'config' is invalid");
            return Some("field 'name' for 'config' is invalid".to_string());
        }
        if self.log_level.is_none() {
            debug!("field 'log_level' for 'config' is invalid");
            return Some("field 'log_level' for 'config' is invalid".to_string());
        }
        None
    }
}

pub trait BobNodeConfig {
    fn get(&self, filename: &String) -> Result<Vec<DataVDisk>, String>;
}

pub struct NodeConfigYaml {}

impl BobNodeConfig for NodeConfigYaml {
    fn get(&self, filename: &String) -> Result<NodeConfig, String> {
        let file: Result<NodeConfig, String> = YamlBobConfigReader{}.get(filename);
        match file {
            Ok(config) => {
                let is_valid = config.validate();
                if is_valid.is_some() {
                    debug!("config is not valid: {}", is_valid.as_ref().unwrap());
                    return Err(format!("config is not valid: {}", is_valid.unwrap()));
                }
                return Ok(config);
            }
            Err(e) => return Err(e),
        }
    }
}
