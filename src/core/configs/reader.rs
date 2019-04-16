use std::fs;
use serde::de::Deserialize;

pub trait Validatable {
    fn validate(&self) -> Option<String>;

    fn aggregate<T: Validatable>(&self, elements: &Vec<T>) -> Option<String> {
        let options = elements
            .iter()
            .filter_map(|elem|elem.validate())
            .collect::<Vec<String>>();
        if options.len() > 0 {
            return Some(
                options
                    .iter()
                    .fold("".to_string(), |acc, x| acc + "\n" + x),
            );
        }
        None
    }
}

pub trait BobConfigReader<T> {
    fn read(&self, filename: &String) -> Result<String, String> {
        let result: Result<String, _> = fs::read_to_string(filename);
        match result {
            Ok(config) => return Ok(config),
            Err(e) => {
                debug!("error on file opening: {}", e);
                return Err(format!("error on file opening: {}", e));
            }
        }
    }
    fn parse(&self, config: &String) -> Result<T, String>;
    fn get(&self, filename: &String) -> Result<T, String>;
    fn get2(&self, filename: &String) -> Result<T, String>;
}

pub struct YamlBobConfigReader {
}

impl<T> BobConfigReader<T> for YamlBobConfigReader 
        where T: for<'de> Deserialize<'de>,
        T: Validatable {
    fn parse(&self, config: &String) -> Result<T, String>{
        let result: Result<T, _> = serde_yaml::from_str(config);
        match result {
            Ok(conf) => return Ok(conf),
            Err(e) => {
                debug!("error on yaml parsing: {}", e);
                return Err(format!("error on yaml parsing: {}", e));
            }
        }
    }
    fn get(&self, filename: &String) -> Result<T, String>{
        let result = (self as &BobConfigReader<T>).read(filename);
        match result {
            Ok(config) => return self.parse(&config),
            Err(e) => return Err(e),
        }
    }

    fn get2(&self, filename: &String) -> Result<T, String> {
        let file: Result<T, String> = self.get(filename);
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
