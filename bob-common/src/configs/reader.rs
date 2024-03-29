use serde::Deserialize;
use tokio::fs::read_to_string;
use super::validation::Validatable;
pub struct YamlBobConfig {}

impl YamlBobConfig {
    pub async fn read(filename: &str) -> Result<String, String> {
        let result: Result<String, _> = read_to_string(filename).await;
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

    pub async fn get<T>(filename: &str) -> Result<T, String>
    where
        T: for<'de> Deserialize<'de> + Validatable,
    {
        let file = Self::read(filename).await?;
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
