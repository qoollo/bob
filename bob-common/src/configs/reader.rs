use serde::Deserialize;
use std::fs::read_to_string;

pub trait Validatable {
    fn validate(&self) -> Result<(), String>;

    fn aggregate(elements: &[impl Validatable]) -> Result<(), String> {
        let options = elements
            .iter()
            .map(|elem| elem.validate())
            .filter_map(Result::err)
            .collect::<Vec<String>>();
        if options.is_empty() {
            Ok(())
        } else {
            Err(options.iter().fold(String::new(), |acc, x| acc + x + "\n"))
        }
    }
}

pub struct YamlBobConfig {}

impl YamlBobConfig {
    pub fn read(filename: &str) -> Result<String, String> {
        let result: Result<String, _> = read_to_string(filename);
        match result {
            Ok(config) => Ok(config),
            Err(e) => {
                debug!("error on file opening: {}", e);
                Err(format!("error on file opening: {}", e))
            }
        }
    }

    pub fn parse<T>(config: &str) -> Result<T, String>
    where
        T: for<'de> Deserialize<'de> + Validatable,
    {
        let result: Result<T, _> = serde_yaml::from_str(config);
        match result {
            Ok(conf) => Ok(conf),
            Err(e) => {
                debug!("error on yaml parsing: {}", e);
                Err(format!("error on yaml parsing: {}", e))
            }
        }
    }

    pub fn get<T>(filename: &str) -> Result<T, String>
    where
        T: for<'de> Deserialize<'de> + Validatable,
    {
        let file = Self::read(filename)?;
        let config: T = Self::parse(&file)?;

        let is_valid = config.validate();
        match is_valid {
            Ok(_) => Ok(config),
            Err(e) => {
                debug!("config is not valid: {}", e);
                Err(format!("config is not valid: {}", e))
            }
        }
    }
}
