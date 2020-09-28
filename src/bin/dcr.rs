#[macro_use]
extern crate derive_new;
#[macro_use]
extern crate serde_derive;
extern crate ctrlc;

use crate::docker_cluster_runner::fs_configuration::FSConfiguration;
use crate::docker_cluster_runner::TestClusterConfiguration;
use clap::{App, Arg};
use std::error::Error;
use std::fmt::Display;
use std::fs;
use std::sync::{Arc, Mutex};

mod docker_cluster_runner;

// Docker cluster runner
// Usage: dcr [-c config.yaml]
// Default config is dcr_config.yaml
// Can be gracefully stopped by ctrl+c
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config_dir = "cluster_test";
    let fs_configuration = FSConfiguration::new(
        ".".to_string(),
        config_dir.to_string(),
        config_dir.to_string(),
        "/tmp".to_string(),
    );
    let configuration =
        get_configuration().map_err(|e| ErrorWrapper::new("configuration get", e))?;
    configuration
        .save_cluster_configuration(config_dir)
        .map_err(|e| ErrorWrapper::new("save cluster configuration", e))?;
    let compose = configuration
        .create_docker_compose(fs_configuration, "bobnet".to_string())
        .map_err(|e| ErrorWrapper::new("create compose configuration", e))?;
    let arc = Arc::new(Mutex::new(Box::new(compose)));
    let ctrlc_arc = arc.clone();
    ctrlc::set_handler(move || {
        let compose = ctrlc_arc.lock().unwrap();
        compose
            .down(config_dir)
            .expect("failed to run \"down\"")
            .wait_with_output()
            .expect("failed to wait for compose down otuput");
    })
    .map_err(|e| ErrorWrapper::new("ctrlc handler", e.into()))?;
    let child_process = arc
        .lock()
        .unwrap()
        .up(config_dir)
        .map_err(|e| ErrorWrapper::new("run docker_compose", e))?;
    child_process
        .wait_with_output()
        .map_err(|e| ErrorWrapper::new("wait for child process", e.into()))?;
    Ok(())
}

fn get_configuration() -> Result<TestClusterConfiguration, Box<dyn Error>> {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .arg(
            Arg::with_name("config")
                .help("config for docker cluster")
                .takes_value(true)
                .short("c")
                .required(true)
                .default_value("dcr_config.yaml")
                .long("config"),
        )
        .get_matches();
    let config_filename = matches.value_of("config").expect("required");
    let file_content = fs::read_to_string(config_filename)?;
    let configuration: TestClusterConfiguration = serde_yaml::from_str(&file_content)?;
    Ok(configuration)
}

#[derive(Debug)]
struct ErrorWrapper {
    description: String,
    inner_error: Box<dyn Error>,
}

impl ErrorWrapper {
    fn new(str: &'static str, err: Box<dyn Error>) -> Self {
        Self {
            description: str.into(),
            inner_error: err,
        }
    }
}

impl Display for ErrorWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Error: {}", self.description)?;
        writeln!(f, "{}", self.inner_error)
    }
}

impl Error for ErrorWrapper {}
