pub enum LogLevel {
    Debug,
    Error,
}

pub struct NodeConfig {
    pub log_level: Option<LogLevel>,
    pub name: Option<String>,
}