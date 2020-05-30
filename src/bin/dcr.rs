use clap::{App, Arg};
use std::collections::HashMap;
use std::error::Error;
use std::io::{Read, Stdout, Write};
use std::net::Ipv4Addr;
use std::process::{Child, Command, Stdio};
use std::thread::spawn;

#[macro_use]
extern crate derive_new;
#[macro_use]
extern crate serde_derive;

#[tokio::main]
async fn main() {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .arg(
            Arg::with_name("config")
                .help("config for docker cluster")
                .takes_value(true)
                .short("c")
                .required(true)
                .long("config"),
        )
        .get_matches();
    let config_filename = matches.value_of("config").expect("required");
    let file_content = std::fs::read_to_string(config_filename);
    if let Ok(file_content) = file_content {
        let configuration: Result<Configuration, _> = serde_yaml::from_str(&file_content);
        if let Ok(configuration) = configuration {}
    }
}

#[derive(Deserialize)]
struct Configuration {
    nodes_count: u32,
    replicas_count: u32,
    disks_count: u32,
    port: u32,
}

#[derive(Serialize, new)]
struct DockerCompose {
    version: String,
    services: HashMap<String, DockerService>,
}

#[derive(Serialize, new)]
struct DockerService {
    build: DockerBuild,
    volumes: Vec<VolumeMapping>,
    command: String,
    networks: HashMap<String, DockerNetwork>,
}

#[derive(Serialize, new)]
struct DockerBuild {
    context: String,
    dockerfile: String,
}

impl DockerBuild {
    fn build(&self) -> Result<Child, Box<dyn Error>> {
        let mut command = Command::new("sudo");
        command
            .arg("docker")
            .arg("build")
            .arg(self.context.clone())
            .arg("-f")
            .arg(self.dockerfile.clone());
        command.spawn().map_err(|e| e.into())
    }
}

#[derive(Serialize, new)]
struct VolumeMapping {
    host_dir: String,
    docker_dir: String,
}

#[derive(Serialize, Clone, Copy, new)]
struct DockerNetwork {
    ipv4_address: Ipv4Addr,
}

mod tests {
    use crate::{DockerBuild, DockerCompose, DockerNetwork, DockerService, VolumeMapping};
    use std::collections::HashMap;
    use std::net::Ipv4Addr;
    use std::str::FromStr;

    #[test]
    fn correctly_serializes_compose() {
        let addr = Ipv4Addr::from_str("192.168.1.1").unwrap();
        let network = DockerNetwork::new(addr);
        let mut networks = HashMap::new();
        networks.insert("test_network".to_string(), network);
        let volume = VolumeMapping::new("host_test".to_string(), "docker_test".to_string());
        let docker_build = DockerBuild::new(".".to_string(), "Dockerfile".to_string());
        let service = DockerService::new(
            docker_build,
            vec![volume],
            "test_command".to_string(),
            networks,
        );
        let mut services = HashMap::new();
        services.insert("test_service".to_string(), service);
        let docker_compose = DockerCompose::new("3.8".to_string(), services);
        let yml = serde_yaml::to_string(&docker_compose);
        assert!(yml.is_ok());
    }

    #[test]
    fn builds_docker() {
        let docker_build = DockerBuild::new(".".to_string(), "cluster_test/Dockerfile".to_string());
        let result = docker_build.build();
        assert!(result.is_ok());
        let output = result.unwrap().wait_with_output();
        eprintln!("output = {:?}", output);
    }
}
