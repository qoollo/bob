use log::LevelFilter;
use std::collections::HashMap;
use std::error::Error;
use std::str::FromStr;

// We need custom logger structs as log4rs ones are not serializable
#[derive(Serialize, new)]
struct LoggerConfigurationFile {
    refresh_rate: String,
    appenders: HashMap<String, AppenderConfiguration>,
    root: LoggerConfiguration,
    loggers: HashMap<String, LoggerConfiguration>,
}

#[derive(Serialize, new)]
struct AppenderConfiguration {
    kind: String,
    encoder: EncoderConfiguration,
}

#[derive(Serialize, new)]
struct EncoderConfiguration {
    pattern: String,
}

#[derive(Serialize)]
struct LoggerConfiguration {
    level: String,
    appenders: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    additive: Option<bool>,
}

impl LoggerConfiguration {
    fn new(level: LevelFilter, appenders: Vec<String>, additive: Option<bool>) -> Self {
        Self {
            level: level.to_string().to_lowercase(),
            appenders,
            additive,
        }
    }
}

pub fn create_logger_yaml(directory: &str, level_string: &String) -> Result<(), Box<dyn Error>> {
    let level = LevelFilter::from_str(level_string)?;
    eprintln!("level = {:?}", level);
    let refresh_rate = "30 seconds";
    let encoder = EncoderConfiguration::new(
        "{d(%Y-%m-%d %H:%M:%S):<20} {M:>20.30}:{L:>3} {h({l})}    {m}\n".to_string(),
    );
    let appender_name = "stdout";
    let appender = AppenderConfiguration::new("console".to_string(), encoder);
    let root = LoggerConfiguration::new(level, vec![appender_name.to_string()], None);
    let bob = LoggerConfiguration::new(level, vec![appender_name.to_string()], Some(false));
    let pearl = LoggerConfiguration::new(level, vec![appender_name.to_string()], Some(false));

    let mut appenders = HashMap::new();
    appenders.insert(appender_name.to_string(), appender);

    let mut loggers = HashMap::new();
    loggers.insert("bob".to_string(), bob);
    loggers.insert("pearl".to_string(), pearl);

    let logger_configuration_file =
        LoggerConfigurationFile::new(refresh_rate.to_string(), appenders, root, loggers);
    let str = serde_yaml::to_string(&logger_configuration_file)?;
    std::fs::write(format!("{}/logger.yaml", directory), str)?;
    Ok(())
}
