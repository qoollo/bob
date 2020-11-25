use crate::docker_cluster_runner::docker_compose_wrapper::*;
use crate::docker_cluster_runner::fs_configuration::FSConfiguration;
use bitflags::_core::cell::RefCell;
use bob::configs::node::BackendSettings;
use bob::configs::{Cluster, ClusterNode, MetricsConfig, Node, Pearl, Replica, VDisk};
use bob::DiskPath;
use filesystem_constants::DockerFSConstants;
use std::cmp::min;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::net::Ipv4Addr;
use std::path::Path;
use std::str::FromStr;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Deserialize, new)]
pub struct TestClusterConfiguration {
    nodes_count: u32,
    vdisks_count: u32,
    logging_level: String,
    ssh_pub_key: String,
    quorum: usize,
    storage_format_type: Option<String>,
    timestamp_period: String,
    cleanup_interval: String,
    open_blobs_soft_limit: Option<usize>,
    open_blobs_hard_limit: Option<usize>,
}

impl TestClusterConfiguration {
    pub fn save_cluster_configuration(&self, directory: &str, ssh_directory: &str) -> Result<()> {
        self.save_cluster_config(directory)?;
        self.save_logger_config(directory)?;
        ssh_generation::populate_ssh_directory(
            ssh_directory.to_string(),
            Some(self.ssh_pub_key.clone()),
        )?;
        self.save_nodes_config(directory)?;
        Ok(())
    }

    fn save_nodes_config(&self, directory: &str) -> Result<()> {
        for node in 0..self.nodes_count {
            self.save_node_config(directory, node)?;
        }
        Ok(())
    }

    fn save_logger_config(&self, directory: &str) -> Result<()> {
        logger::create_logger_yaml(directory, &self.logging_level)
    }

    fn save_node_config(&self, directory: &str, node: u32) -> Result<()> {
        let (name, node) = self.create_named_node_configuration(node);
        let node_string = serde_yaml::to_string(&node)?;
        fs::write(format!("{}/{}.yaml", directory, name), node_string)?;
        Ok(())
    }

    fn save_cluster_config(&self, directory: &str) -> Result<()> {
        let cluster = self.create_cluster();
        let cluster_string = serde_yaml::to_string(&cluster)?;
        fs::write(format!("{}/cluster.yaml", directory), cluster_string)?;
        Ok(())
    }

    pub fn create_docker_compose(
        &self,
        fs_configuration: FSConfiguration,
        network_name: String,
    ) -> Result<DockerCompose> {
        let mut services = HashMap::with_capacity(self.nodes_count as usize);
        for node in 0..self.nodes_count {
            services.insert(
                Self::get_node_name(node),
                DockerService::new(
                    Self::get_build_command(&fs_configuration)?,
                    Self::get_volumes(&fs_configuration)?,
                    Self::get_docker_command(node),
                    Self::get_networks_with_single_network(node, &network_name),
                    Self::get_ports(node),
                    Self::get_docker_env(node),
                    Self::get_security_opts(&fs_configuration)?,
                    Self::get_ulimits(),
                ),
            );
        }
        let networks = Self::get_networks_with_custom_network(network_name);
        let compose = DockerCompose::new("3.8".to_string(), services, networks);
        Ok(compose)
    }

    fn get_docker_env(node: u32) -> Vec<DockerEnv> {
        vec![
            DockerEnv::new(
                "CONFIG_DIR".to_string(),
                Some(DockerFSConstants::docker_configs_dir()),
            ),
            DockerEnv::new("NODE_NAME".to_string(), Some(Self::get_node_name(node))),
        ]
    }

    fn get_ports(node: u32) -> Vec<DockerPort> {
        vec![
            DockerPort::new(8000, 8000 + node),
            DockerPort::new(22, 7022 + node),
        ]
    }

    fn get_ulimits() -> ULimits {
        ULimits::new(4194304000, FileLimits::new(98304, 98304))
    }

    fn get_security_opts(fs_configuration: &FSConfiguration) -> Result<Vec<SecurityOpt>> {
        let filename = format!(
            "{}/profile.json",
            Self::convert_to_absolute_path(&fs_configuration.cluster_configuration_dir)?
        );
        if !Path::new(&filename).exists() {
            let content = "{ \"syscalls\": [] }";
            fs::write(&filename, content)?;
        }
        Ok(vec![SecurityOpt::new(filename)])
    }

    fn get_volumes(fs_configuration: &FSConfiguration) -> Result<Vec<VolumeMapping>> {
        let config_dir =
            Self::convert_to_absolute_path(&fs_configuration.cluster_configuration_dir)?;
        let disks_dir = Self::convert_to_absolute_path(&fs_configuration.disks_dir)?;
        let ssh_dir = Self::convert_to_absolute_path(&fs_configuration.ssh_dir())?;
        let volumes = vec![
            VolumeMapping::new(disks_dir, DockerFSConstants::docker_disks_dir()),
            VolumeMapping::new(config_dir, DockerFSConstants::docker_configs_dir()),
            VolumeMapping::new(ssh_dir, DockerFSConstants::docker_ssh_dir()),
        ];
        Ok(volumes)
    }

    fn get_build_command(fs_configuration: &FSConfiguration) -> Result<DockerBuild> {
        let bob_dir = Self::convert_to_absolute_path(&fs_configuration.bob_source_dir)?;
        let build = DockerBuild::new(
            bob_dir,
            format!("{}/Dockerfile", fs_configuration.dockerfile_path),
        );
        Ok(build)
    }

    fn get_networks_with_custom_network(
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

    fn convert_to_absolute_path(dir: &str) -> Result<String> {
        fs::canonicalize(dir)
            .map(|p| p.to_str().unwrap().to_string())
            .map_err(|e| e.into())
    }

    fn get_networks_with_single_network(
        node: u32,
        network_name: &str,
    ) -> HashMap<String, DockerNetwork> {
        let network = DockerNetwork::new(Ipv4Addr::from_str(&Self::get_node_addr(node)).unwrap());
        let mut networks = HashMap::with_capacity(1);
        networks.insert(network_name.to_string(), network);
        networks
    }

    fn get_docker_command(node: u32) -> String {
        let command = format!("cluster.yaml {}.yaml", Self::get_node_name(node));
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
            self.quorum,
            "3sec".to_string(),
            "5000ms".to_string(),
            "quorum".to_string(),
            "pearl".to_string(),
            Some(self.get_pearl_config(node_index)),
            Some(MetricsConfig::new(
                "bob".to_string(),
                "127.0.0.1:2003".to_string(),
            )),
            RefCell::default(),
            RefCell::default(),
            self.cleanup_interval.clone(),
            self.open_blobs_soft_limit,
            self.open_blobs_hard_limit,
        );
        (Self::get_node_name(node_index), node)
    }

    fn get_pearl_config(&self, node: u32) -> Pearl {
        Pearl::new(
            1000000,
            1000000,
            "bob".to_string(),
            "100ms".to_string(),
            3,
            Self::get_disk_name(
                (0..self.vdisks_count)
                    .rev()
                    .find(|&v| self.vdisk_allowed(node, v))
                    .unwrap_or_else(|| panic!("no disks for node {}", node)),
            ),
            true,
            BackendSettings::new(
                "bob".to_string(),
                "alien".to_string(),
                self.timestamp_period.clone(),
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
                if self.vdisk_allowed(j, i) {
                    vdisk.push_replica(Replica::new(node_name, disk_name));
                }
            }
            result.push(vdisk);
        }
        result
    }

    fn vdisk_allowed(&self, node_index: u32, disk_index: u32) -> bool {
        match self.storage_format_type.as_deref() {
            Some("parity") => return node_index % 2 == disk_index % 2,
            Some("spread") => {
                let disks = (node_index..self.vdisks_count)
                    .chain(0..node_index)
                    .flat_map(|i| vec![i; self.quorum]);
                let step = self.nodes_count as usize;
                return disks.step_by(step).any(|i| i == disk_index);
            }
            Some(s) => {
                if &s[0..1] == "n" {
                    let count = s[1..].parse().unwrap_or(self.vdisks_count);
                    if count < self.vdisks_count {
                        let prev = (count * node_index) % self.vdisks_count;
                        let ranges = [
                            (prev..min(self.vdisks_count, prev + count)),
                            (0..(prev + count).saturating_sub(self.vdisks_count)),
                        ];
                        return ranges.iter().any(|r| r.contains(&disk_index));
                    }
                }
            }
            _ => (),
        }
        true
    }

    fn create_disk_paths(&self, node_index: u32) -> Vec<DiskPath> {
        let mut result = Vec::with_capacity(self.vdisks_count as usize);
        for i in 0..self.vdisks_count {
            if self.vdisk_allowed(node_index, i) {
                result.push(Self::get_disk_path(node_index, i));
            }
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

    impl FSConfiguration {
        pub fn ssh_dir(&self) -> String {
            format!("{}/ssh", self.cluster_configuration_dir)
        }
    }
}

mod filesystem_constants;

pub mod docker_compose_wrapper;

mod logger;
mod ssh_generation;

#[cfg(test)]
mod tests {
    use super::TestClusterConfiguration;

    #[test]
    fn creates_cluster_configuration_for_two_nodes() {
        let configuration = TestClusterConfiguration::new(
            2,
            2,
            "warn".to_string(),
            String::new(),
            0,
            None,
            "1d".to_string(),
            "1d".to_string(),
            None,
            None,
        );
        let cluster = configuration.create_cluster();
        assert_eq!(cluster.nodes().len(), 2, "wrong nodes count");
        assert_eq!(cluster.vdisks().len(), 2, "wrong vdisks count");
    }
}
