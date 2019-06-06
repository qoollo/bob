use serde::de::Deserialize;
use std::fs;

pub trait Validatable {
    fn validate(&self) -> Option<String>;

    fn aggregate<T: Validatable>(&self, elements: &[T]) -> Option<String> {
        let options = elements
            .iter()
            .filter_map(|elem| elem.validate())
            .collect::<Vec<String>>();
        if !options.is_empty() {
            return Some(options.iter().fold("".to_string(), |acc, x| acc + "\n" + x));
        }
        None
    }
}

pub struct YamlBobConfigReader {}

impl YamlBobConfigReader {
    pub fn read(&self, filename: &str) -> Result<String, String> {
        let result: Result<String, _> = fs::read_to_string(filename);
        match result {
            Ok(config) => Ok(config),
            Err(e) => {
                debug!("error on file opening: {}", e);
                Err(format!("error on file opening: {}", e))
            }
        }
    }

    pub fn parse<T>(&self, config: &str) -> Result<T, String>
    where
        T: for<'de> Deserialize<'de>,
        T: Validatable,
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

    pub fn get<T>(&self, filename: &str) -> Result<T, String>
    where
        T: for<'de> Deserialize<'de>,
        T: Validatable,
    {
        let file = self.read(filename)?;
        let config: T = self.parse(&file)?;

        let is_valid = config.validate();
        if is_valid.is_some() {
            debug!("config is not valid: {}", is_valid.as_ref().unwrap());
            return Err(format!("config is not valid: {}", is_valid.unwrap()));
        }
        Ok(config)
    }
}
