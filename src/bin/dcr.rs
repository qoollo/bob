use bitflags::_core::cell::RefCell;
use bob::configs::node::BackendSettings;
use bob::configs::{Cluster, ClusterNode, MetricsConfig, Node, Pearl, Replica, VDisk};
use bob::DiskPath;
use clap::{App, Arg};
use serde::{Serialize, Serializer};
use std::collections::HashMap;
use std::error::Error;
use std::io::{Read, Stdout, Write};
use std::net::Ipv4Addr;
use std::process::{Child, Command, Stdio};
use std::str::FromStr;
use std::thread::spawn;

#[macro_use]
extern crate derive_new;
#[macro_use]
extern crate serde_derive;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let configuration = get_configuration()?;
    let config_dir = "cluster_test";
    save_configuration(config_dir, &configuration)?;
    let compose = configuration.create_docker_compose(config_dir, "cluster_test/Dockerfile");
    let compose_string = serde_yaml::to_string(&compose)?;
    std::fs::write(format!("{}/docker-compose.yml", config_dir), compose_string)?;
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
                .long("config"),
        )
        .get_matches();
    let config_filename = matches.value_of("config").expect("required");
    let file_content = std::fs::read_to_string(config_filename)?;
    let configuration: TestClusterConfiguration = serde_yaml::from_str(&file_content)?;
    Ok(configuration)
}

fn save_configuration(
    base_dir: &str,
    configuration: &TestClusterConfiguration,
) -> Result<(), Box<dyn Error>> {
    let cluster_configuration = configuration.create_cluster_configuration();
    let cluster_configuration = serde_yaml::to_string(&cluster_configuration)?;
    std::fs::write(
        format!(
            "{}/{}.yaml",
            base_dir,
            TestClusterConfiguration::cluster_filename_without_extension()
        ),
        cluster_configuration,
    )?;
    for i in 0..configuration.nodes_count {
        let (name, node) = configuration.create_named_node_configuration(i);
        let node_configuration = serde_yaml::to_string(&node)?;
        std::fs::write(format!("{}/{}.yaml", base_dir, name), node_configuration)?;
    }
    Ok(())
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

    fn create_named_node_configuration(&self, node_index: u32) -> (String, Node) {
        let node = Node::new(
            "/configs/logger.yaml".to_string(),
            Self::get_node_name(node_index),
            1,
            "3sec".to_string(),
            "5000ms".to_string(),
            "simple".to_string(),
            "pearl".to_string(),
            Some(self.get_pearl_config()),
            Some(MetricsConfig::new(
                "test".to_string(),
                "127.0.0.1:8888".to_string(),
            )),
            RefCell::default(),
            RefCell::default(),
        );
        (Self::get_node_name(node_index), node)
    }

    fn create_docker_compose(&self, config_dir: &str, dockerfile: &str) -> DockerCompose {
        let build = DockerBuild::new(
            std::fs::canonicalize(".")
                .unwrap()
                .to_str()
                .unwrap()
                .to_string(),
            dockerfile.to_string(),
        );
        let volumes = vec![
            VolumeMapping::new("/tmp".to_string(), "/tmp".to_string()),
            VolumeMapping::new(
                std::fs::canonicalize(config_dir)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string(),
                "/configs".to_string(),
            ),
        ];
        let mut services = HashMap::with_capacity(self.nodes_count as usize);
        for node in 0..self.nodes_count {
            let command = format!(
                "./bobd -c /configs/{}.yaml -n /configs/{}.yaml",
                Self::cluster_filename_without_extension(),
                Self::get_node_name(node)
            );
            let network =
                DockerNetwork::new(Ipv4Addr::from_str(&Self::get_node_addr(node)).unwrap());
            let mut networks = HashMap::with_capacity(1);
            networks.insert("bobnet".to_string(), network);
            services.insert(
                Self::get_node_name(node),
                DockerService::new(build.clone(), volumes.clone(), command, networks),
            );
        }
        let mut networks = HashMap::with_capacity(1);
        networks.insert(
            "bobnet".to_string(),
            DockerComposeNetwork::new(
                "bridge".to_string(),
                IPAMConfiguration::new(vec![SubnetConfiguration::new(
                    "192.168.17.0/24".to_string(),
                )]),
            ),
        );
        DockerCompose::new("3.8".to_string(), services, networks)
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
            let address = format!("{}:20000", Self::get_node_addr(i));
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
        format!("192.168.17.{}", node_index + 10)
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

    const fn cluster_filename_without_extension() -> &'static str {
        "cluster"
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
    networks: HashMap<String, DockerComposeNetwork>,
}

#[derive(Serialize, new)]
struct DockerService {
    build: DockerBuild,
    volumes: Vec<VolumeMapping>,
    command: String,
    networks: HashMap<String, DockerNetwork>,
}

#[derive(Serialize, new, Clone)]
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

#[derive(new, Clone)]
struct VolumeMapping {
    host_dir: String,
    docker_dir: String,
}

impl Serialize for VolumeMapping {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}:{}", self.host_dir, self.docker_dir))
    }
}

#[derive(Serialize, Clone, Copy, new)]
struct DockerNetwork {
    ipv4_address: Ipv4Addr,
}

#[derive(Serialize, new)]
struct DockerComposeNetwork {
    driver: String,
    ipam: IPAMConfiguration,
}

#[derive(Serialize, new)]
struct IPAMConfiguration {
    config: Vec<SubnetConfiguration>,
}

#[derive(Serialize, new)]
struct SubnetConfiguration {
    subnet: String,
}

mod tests {
    #[test]
    fn creates_cluster_configuration_for_two_nodes() {
        let configuration = TestClusterConfiguration::new(2, 1, 2, 80);
        let cluster = configuration.create_cluster_configuration();
        assert_eq!(cluster.nodes().len(), 2, "wrong nodes count");
        assert_eq!(cluster.vdisks().len(), 2, "wrong vdisks count");
    }
}
