use std::fs;
use serde::de::Deserialize;

pub trait Validatable {
    fn validate(&self) -> Option<String>;

    fn aggregate<T: Validatable>(&self, elements: &Option<Vec<T>>) -> Option<String> {
        let options: Vec<Option<String>> = elements
            .as_ref()?
            .iter()
            .map(|elem| elem.validate())
            .filter(|f| f.is_some())
            .collect::<Vec<Option<String>>>();
        if options.len() > 0 {
            return Some(
                options
                    .iter()
                    .fold("".to_string(), |acc, x| acc + &x.as_ref().unwrap()),
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
}

pub struct YamlBobConfigReader {
}

impl<T> BobConfigReader<T> for YamlBobConfigReader where T: for<'de> Deserialize<'de> {
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
}
