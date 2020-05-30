use bitflags::_core::cell::RefCell;
use bob::configs::node::BackendSettings;
use bob::configs::{Cluster, ClusterNode, Node, Pearl, Replica, VDisk};
use bob::DiskPath;
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
        let configuration: Result<TestClusterConfiguration, _> =
            serde_yaml::from_str(&file_content);
        if let Ok(configuration) = configuration {}
    }
}

#[derive(Deserialize, new)]
struct TestClusterConfiguration {
    nodes_count: u32,
    replicas_count: u32,
    vdisks_count: u32,
    port: u32,
}

impl TestClusterConfiguration {
    fn create_cluster_configuration(&self) -> Cluster {
        let nodes = self.create_nodes();
        let vdisks = self.create_vdisks();
        Cluster::new(nodes, vdisks)
    }

    fn create_named_node_config(&self, node_index: u32) -> (String, Node) {
        Node::new(
            "logger.yaml".to_string(),
            Self::get_node_name(node_index),
            1,
            "3sec".to_string(),
            "5000ms".to_string(),
            "simple".to_string(),
            "pearl".to_string(),
            Some(self.get_pearl_config()),
            None,
            RefCell::default(),
            RefCell::default(),
        )
    }

    fn get_pearl_config(&self) -> Pearl {
        Pearl::new(
            1000000,
            10000,
            "bob".to_string(),
            "100ms".to_string(),
            3,
            Self::get_disk_name(self.vdisks_count - 1),
            true,
            BackendSettings::new(
                "bob".to_string(),
                "alien".to_string(),
                "1d".to_string(),
                "100ms".to_string(),
            ),
        )
    }

    fn create_nodes(&self) -> Vec<ClusterNode> {
        let mut result = Vec::with_capacity(self.nodes_count as usize);
        for i in 0..self.nodes_count {
            let name = Self::get_node_name(i);
            let address = Self::get_node_addr(i);
            let disk_paths = self.create_disk_paths(i);
            result.push(ClusterNode::new(name, address, disk_paths));
        }
        result
    }

    fn create_vdisks(&self) -> Vec<VDisk> {
        let mut result = Vec::with_capacity(self.vdisks_count as usize);
        for i in 0..self.vdisks_count {
            let mut vdisk = VDisk::new(i);
            for j in 0..self.nodes_count {
                let node_name = Self::get_node_name(j);
                let disk_name = Self::get_disk_name(i);
                vdisk.push_replica(Replica::new(node_name, disk_name));
            }
            result.push(vdisk);
        }
        result
    }

    fn create_disk_paths(&self, node_index: u32) -> Vec<DiskPath> {
        let mut result = Vec::with_capacity(self.vdisks_count as usize);
        for i in 0..self.vdisks_count {
            result.push(Self::get_disk_path(node_index, i));
        }
        result
    }

    fn get_node_addr(node_index: u32) -> String {
        format!("192.168.1.{}", node_index)
    }

    fn get_disk_path(node_index: u32, disk_index: u32) -> DiskPath {
        DiskPath::new(
            Self::get_disk_name(disk_index),
            format!(
                "{}/node_{}/disk_{}",
                Self::base_disk_dir(),
                node_index,
                disk_index
            ),
        )
    }

    fn get_disk_name(disk_index: u32) -> String {
        format!("disk_{}", disk_index)
    }

    fn get_node_name(i: u32) -> String {
        format!("node_{}", i)
    }

    const fn base_disk_dir() -> &'static str {
        "/tmp"
    }
}

#[derive(new)]
struct TestCluster {
    cluster: Cluster,
    docker_compose: DockerCompose,
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
    use crate::{
        DockerBuild, DockerCompose, DockerNetwork, DockerService, TestClusterConfiguration,
        VolumeMapping,
    };
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

    #[test]
    fn creates_cluster_configuration_for_two_nodes() {
        let configuration = TestClusterConfiguration::new(2, 1, 2, 80);
        let cluster = configuration.create_cluster_configuration();
        assert_eq!(cluster.nodes().len(), 2, "wrong nodes count");
        assert_eq!(cluster.vdisks().len(), 2, "wrong vdisks count");
    }
}
