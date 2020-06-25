use crate::docker_cluster_runner::docker_compose_wrapper::*;
use crate::docker_cluster_runner::fs_configuration::FSConfiguration;
use bitflags::_core::cell::RefCell;
use bob::configs::node::BackendSettings;
use bob::configs::{Cluster, ClusterNode, MetricsConfig, Node, Pearl, Replica, VDisk};
use bob::DiskPath;
use filesystem_constants::DockerFSConstants;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::net::Ipv4Addr;
use std::str::FromStr;

#[derive(Deserialize, new)]
pub struct TestClusterConfiguration {
    nodes_count: u32,
    vdisks_count: u32,
    level: String,
}

impl TestClusterConfiguration {
    pub fn save_cluster_configuration(&self, directory: &str) -> Result<(), Box<dyn Error>> {
        let cluster = self.create_cluster();
        let cluster_string = serde_yaml::to_string(&cluster)?;
        fs::write(format!("{}/cluster.yaml", directory), cluster_string)?;
        logger::create_logger_yaml(directory, &self.level)?;
        for node in 0..self.nodes_count {
            let (name, node) = self.create_named_node_configuration(node);
            let node_string = serde_yaml::to_string(&node)?;
            fs::write(format!("{}/{}.yaml", directory, name), node_string)?;
        }
        Ok(())
    }

    pub fn create_docker_compose(
        &self,
        fs_configuration: FSConfiguration,
        network_name: String,
    ) -> Result<DockerCompose, Box<dyn Error>> {
        let bob_dir = Self::convert_to_absolute_path(&fs_configuration.bob_source_dir)?;
        let config_dir =
            Self::convert_to_absolute_path(&fs_configuration.cluster_configuration_dir)?;
        let disks_dir = Self::convert_to_absolute_path(&fs_configuration.disks_dir)?;
        let build = DockerBuild::new(
            bob_dir,
            format!("{}/Dockerfile", fs_configuration.dockerfile_path),
        );
        let volumes = vec![
            VolumeMapping::new(disks_dir, DockerFSConstants::docker_disks_dir()),
            VolumeMapping::new(config_dir.clone(), DockerFSConstants::docker_configs_dir()),
        ];
        let mut services = HashMap::with_capacity(self.nodes_count as usize);
        for node in 0..self.nodes_count {
            let command = Self::create_docker_command(node);
            let networks = Self::create_networks_with_single_network(node, network_name.clone());
            services.insert(
                Self::get_node_name(node),
                DockerService::new(build.clone(), volumes.clone(), command, networks),
            );
        }
        let networks = Self::create_networks_with_custom_network(network_name);
        let compose = DockerCompose::new("3.8".to_string(), services, networks);
        Ok(compose)
    }

    fn create_networks_with_custom_network(
        network_name: String,
    ) -> HashMap<String, DockerComposeNetwork> {
        let mut networks = HashMap::with_capacity(1);
        networks.insert(
            network_name,
            DockerComposeNetwork::new(
                "bridge".to_string(),
                IPAMConfiguration::new(vec![SubnetConfiguration::new(format!(
                    "{}0/24",
                    Self::ip_addr_prefix()
                ))]),
            ),
        );
        networks
    }

    fn convert_to_absolute_path(dir: &str) -> Result<String, Box<dyn Error>> {
        fs::canonicalize(dir)
            .map(|p| p.to_str().unwrap().to_string())
            .map_err(|e| e.into())
    }

    fn create_networks_with_single_network(
        node: u32,
        network_name: String,
    ) -> HashMap<String, DockerNetwork> {
        let network = DockerNetwork::new(Ipv4Addr::from_str(&Self::get_node_addr(node)).unwrap());
        let mut networks = HashMap::with_capacity(1);
        networks.insert(network_name, network);
        networks
    }

    fn create_docker_command(node: u32) -> String {
        let command = format!(
            "./bobd -c {}/cluster.yaml -n {}/{}.yaml",
            DockerFSConstants::docker_configs_dir(),
            DockerFSConstants::docker_configs_dir(),
            Self::get_node_name(node)
        );
        command
    }

    fn create_cluster(&self) -> Cluster {
        let nodes = self.create_nodes();
        let vdisks = self.create_vdisks();
        Cluster::new(nodes, vdisks)
    }

    fn create_named_node_configuration(&self, node_index: u32) -> (String, Node) {
        let node = Node::new(
            format!("{}/logger.yaml", DockerFSConstants::docker_configs_dir()),
            Self::get_node_name(node_index),
            1,
            "3sec".to_string(),
            "5000ms".to_string(),
            "quorum".to_string(),
            "pearl".to_string(),
            Some(self.get_pearl_config()),
            Some(MetricsConfig::new(
                "bob".to_string(),
                "127.0.0.1:2003".to_string(),
            )),
            RefCell::default(),
            RefCell::default(),
        );
        (Self::get_node_name(node_index), node)
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
            10,
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
        format!("{}{}", Self::ip_addr_prefix(), node_index + 10)
    }

    fn get_disk_path(node_index: u32, disk_index: u32) -> DiskPath {
        DiskPath::new(
            Self::get_disk_name(disk_index),
            format!(
                "{}/node_{}/disk_{}",
                DockerFSConstants::docker_disks_dir(),
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

    const fn ip_addr_prefix() -> &'static str {
        "192.168.17."
    }
}

pub mod fs_configuration {
    #[derive(new)]
    pub struct FSConfiguration {
        pub(super) bob_source_dir: String,
        pub(super) cluster_configuration_dir: String,
        pub(super) dockerfile_path: String,
        pub(super) disks_dir: String,
    }
}

mod filesystem_constants;

pub mod docker_compose_wrapper;

mod logger;

mod tests {
    use super::TestClusterConfiguration;

    #[test]
    fn creates_cluster_configuration_for_two_nodes() {
        let configuration = TestClusterConfiguration::new(2, 2);
        let cluster = configuration.create_cluster();
        assert_eq!(cluster.nodes().len(), 2, "wrong nodes count");
        assert_eq!(cluster.vdisks().len(), 2, "wrong vdisks count");
    }
}
