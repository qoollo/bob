use serde::de::Deserialize;
use std::fs;

pub trait Validatable {
    fn validate(&self) -> Option<String>;

    fn aggregate<T: Validatable>(&self, elements: &Vec<T>) -> Option<String> {
        let options = elements
            .iter()
            .filter_map(|elem| elem.validate())
            .collect::<Vec<String>>();
        if options.len() > 0 {
            return Some(options.iter().fold("".to_string(), |acc, x| acc + "\n" + x));
        }
        None
    }
}

pub trait BobConfigReader {
    fn read(&self, filename: &str) -> Result<String, String> {
        let result: Result<String, _> = fs::read_to_string(filename);
        match result {
            Ok(config) => return Ok(config),
            Err(e) => {
                debug!("error on file opening: {}", e);
                return Err(format!("error on file opening: {}", e));
            }
        }
    }
    fn parse<T>(&self, config: &str) -> Result<T, String>
    where
        T: for<'de> Deserialize<'de>,
        T: Validatable;
    fn get<T>(&self, filename: &str) -> Result<T, String>
    where
        T: for<'de> Deserialize<'de>,
        T: Validatable;
}

pub struct YamlBobConfigReader {}

impl BobConfigReader for YamlBobConfigReader {
    fn parse<T>(&self, config: &str) -> Result<T, String>
    where
        T: for<'de> Deserialize<'de>,
        T: Validatable,
    {
        let result: Result<T, _> = serde_yaml::from_str(config);
        match result {
            Ok(conf) => return Ok(conf),
            Err(e) => {
                debug!("error on yaml parsing: {}", e);
                return Err(format!("error on yaml parsing: {}", e));
            }
        }
    }
    fn get<T>(&self, filename: &str) -> Result<T, String>
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
        return Ok(config);
    }
}
