use super::{
    node::BackendType,
    reader::YamlBobConfig,
    validation::{Validatable, Validator},
};
use crate::{
    configs::node::Node as NodeConfig,
    core_types::{DiskName, DiskPath, NodeDisk, VDiskId},
    node::NodeName,
};
use anyhow::{anyhow, Result as AnyResult};
use http::Uri;
use std::collections::{HashMap, HashSet};

impl Validatable for DiskPath {
    fn validate(&self) -> Result<(), String> {
        // For some reason serde yaml deserializes "field: # no value" into '~'
        if self.name().as_str().is_empty()
            || self.path().is_empty()
            || self.name() == "~"
            || self.path() == "~"
        {
            Err("NodeDisk validation failed: node disks must contain not empty fields 'name' and 'path'".to_string())
        } else {
            Ok(())
        }
    }
}

/// Rack config struct, with name and [`Node`] names.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Rack {
    name: String,
    nodes: Vec<String>,
}

impl Rack {
    /// Created rack with given name and empty nodes list
    #[inline]
    #[must_use]
    pub fn new(name: impl Into<String>) -> Rack {
        Rack {
            name: name.into(),
            nodes: vec![],
        }
    }

    /// Returns rack name, empty if name wasn't set in config.
    #[inline]
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns slice with [`Node`] names.
    #[inline]
    #[must_use]
    pub fn nodes(&self) -> &[String] {
        &self.nodes
    }
}

impl Validatable for Rack {
    fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() || self.name == "~" {
            return Err("rack must contain not empty field 'name'".to_string());
        }

        Validator::validate_no_duplicates(self.nodes.iter()).map_err(|dup_item| {
            format!("rack '{}' can't contain identical node names: {}", self.name(), dup_item)
        })?;

        Ok(())
    }
}

/// Node config struct, with name, address and [`DiskPath`]s.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Node {
    name: String,
    address: String,
    disks: Vec<DiskPath>,
}

impl Node {
    pub fn new(name: String, address: String, disks: Vec<DiskPath>) -> Node {
        Node {
            name,
            address,
            disks,
        }
    }
    /// Returns node name, empty if name wasn't set in config.
    #[inline]
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns slice of disk configs [`DiskPath`].
    #[inline]
    #[must_use]
    pub fn disks(&self) -> &[DiskPath] {
        &self.disks
    }

    /// Returns node address, empty if address wasn't set in config.
    #[inline]
    #[must_use]
    pub fn address(&self) -> &str {
        &self.address
    }
}

impl Validatable for Node {
    fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() || self.name == "~" {
            return Err("node must contain not empty field 'name'".to_string());
        }

        match self.address.parse::<Uri>() {
            Ok(uri) => {
                if uri.port_u16().is_none() {
                    return Err(format!("{}: node uri missing port", uri));
                }
            }
            Err(e) => {
                return Err(format!("{}: node uri parse failed: {}", self.address, e));
            }
        }

        Validator::aggregate(&self.disks)?;

        Validator::validate_no_duplicates(self.disks.iter().map(|dp| dp.name())).map_err(|dup_item| {
            format!("node '{}' can't use identical names for disks: {}", self.name(), dup_item)
        })?;

        Ok(())
    }
}

/// Struct represents replica info for virtual disk.
#[derive(Debug, PartialEq, Serialize, Deserialize, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct Replica {
    node: String,
    disk: String,
}

impl Replica {
    /// Creates new replica struct with given node name and disk name.
    #[must_use]
    pub fn new(node: String, disk: String) -> Self {
        Self { node, disk }
    }

    /// Returns node name, empty if name wasn't set in config.
    #[must_use]
    pub fn node(&self) -> &str {
        &self.node
    }

    #[must_use]
    pub fn disk(&self) -> &str {
        &self.disk
    }
}

impl Validatable for Replica {
    fn validate(&self) -> Result<(), String> {
        if self.node.is_empty() || self.disk.is_empty() {
            Err("replica must contain not empty fields 'node' and 'disk'".to_string())
        } else {
            Ok(())
        }
    }
}

/// Config for virtual disks, stores replicas locations.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct VDisk {
    id: u32,
    #[serde(default)]
    replicas: Vec<Replica>,
}

impl VDisk {
    /// Creates new instance of the [`VDisk`] with given id. To add replicas use [`push_replicas`].
    #[must_use]
    pub fn new(id: u32) -> Self {
        Self {
            id,
            replicas: Vec::new(),
        }
    }

    /// Returns [`VDisk`] id, panics if vdisk was initialized from config and no id was set.
    #[must_use]
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Returns slice with replicas of the [`VDisk`]
    #[must_use]
    pub fn replicas(&self) -> &[Replica] {
        &self.replicas
    }

    /// Adds new replica to the [`VDisk`]
    pub fn push_replica(&mut self, replica: Replica) {
        self.replicas.push(replica)
    }
}

impl Validatable for VDisk {
    fn validate(&self) -> Result<(), String> {
        if self.replicas.is_empty() {
            return Err(format!("vdisk must have replicas: {}", self.id()));
        }
        Validator::aggregate(&self.replicas).map_err(|e| {
            format!("vdisk {} is invalid: {}", self.id(), e)
        })?;

        Validator::validate_no_duplicates(self.replicas.iter()).map_err(|dup_item| {
            format!("vdisk: {} contains duplicate replicas: ({}, {})", self.id(), dup_item.node(), dup_item.disk())
        })?;

        Ok(())
    }
}

/// Distribution function type for cluster
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Copy)]
pub enum DistributionFunc {
    Mod,
}

impl Default for DistributionFunc {
    fn default() -> Self {
        DistributionFunc::Mod
    }
}

/// Config with cluster structure description.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Cluster {
    #[serde(default)]
    nodes: Vec<Node>,
    #[serde(default)]
    vdisks: Vec<VDisk>,
    #[serde(default)]
    racks: Vec<Rack>,
    #[serde(default)]
    distribution_func: DistributionFunc,
}

impl Cluster {
    pub fn new(
        nodes: Vec<Node>,
        vdisks: Vec<VDisk>,
        racks: Vec<Rack>,
        distribution_func: DistributionFunc,
    ) -> Cluster {
        Cluster {
            nodes,
            vdisks,
            racks,
            distribution_func,
        }
    }
    /// Returns slice with [`Node`]s.
    #[must_use]
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    /// Returns slice with [`VDisk`]s.
    #[must_use]
    pub fn vdisks(&self) -> &[VDisk] {
        &self.vdisks
    }

    /// Returns distribution function
    #[must_use]
    pub fn distribution_func(&self) -> DistributionFunc {
        self.distribution_func
    }

    /// Returns slice with [`Rack`]s.
    #[must_use]
    pub fn racks(&self) -> &[Rack] {
        &self.racks
    }

    /// Extends the vdisks collection with contents of the iterator.
    pub fn vdisks_extend(&mut self, iter: impl IntoIterator<Item=VDisk>) {
        self.vdisks.extend(iter)
    }

    /// Creates map with [`NodeDisk`] collection for every [`VDiskId`] from config, required for mapper.
    /// # Errors
    /// Returns error description. If can't match node name with replica name.
    /// And if node disk with replica disk.
    pub fn collect_vdisk_replicas(&self) -> Result<HashMap<VDiskId, Vec<NodeDisk>>, String> {
        let mut vdisks = HashMap::new();
        for vdisk in &self.vdisks {
            let mut physical_disks = Vec::new();
            for replica in vdisk.replicas() {
                let disk_name = DiskName::from(replica.disk());
                let node: &Node = self
                    .nodes
                    .iter()
                    .find(|node| node.name() == replica.node())
                    .ok_or("unknown node name in replica")?;
                let disk_path = node
                    .disks()
                    .iter()
                    .find(|disk| *disk.name() == disk_name)
                    .ok_or_else(|| {
                        format!(
                            "can't find disk with name [{}], check replica section of vdisk [{}]",
                            disk_name,
                            vdisk.id()
                        )
                    })?
                    .path();
                let node_disk = NodeDisk::new(disk_path, disk_name, NodeName::from(node.name()));
                physical_disks.push(node_disk);
            }
            vdisks.insert(vdisk.id() as VDiskId, physical_disks);
        }
        Ok(vdisks)
    }

    /// Loads config from disk, and validates it.
    /// # Errors
    /// IO errors and failed validation.
    pub async fn try_get(filename: &str) -> Result<Self, String> {
        let config = YamlBobConfig::get::<Cluster>(filename).await?;
        if let Err(e) = config.validate() {
            debug!("Cluster config is not valid: {}", e);
            Err(format!("Cluster config is not valid: {}", e))
        } else {
            Ok(config)
        }
    }

    /// Read and validate node config file.
    /// # Errors
    /// IO errors, failed validation and cluster structure check.
    pub async fn get(&self, filename: &str) -> AnyResult<NodeConfig> {
        let config = YamlBobConfig::get::<NodeConfig>(filename)
            .await
            .map_err(|e| anyhow::anyhow!("failed to parse NodeConfig from yaml: {}", e))?;

        if let Err(e) = config.validate() {
            debug!("Node config is not valid: {}", e);
            Err(anyhow::anyhow!("Node config is not valid: {}", e))
        } else {
            self.check(&config)
                .map_err(|e| anyhow::anyhow!("node config check failed: {}", e))?;
            Ok(config)
        }
    }

    pub fn check(&self, node: &NodeConfig) -> Result<(), String> {
        let finded = self
            .nodes()
            .iter()
            .find(|n| n.name() == node.name())
            .ok_or_else(|| {
                debug!("cannot find node: {} in cluster config", node.name());
                format!("cannot find node: {} in cluster config", node.name())
            })?;
        if let Some(bad_vdisk) = self
            .vdisks()
            .iter()
            .find(|vdisk| vdisk.replicas().len() < node.quorum())
        {
            return Err(format!(
                "Quorum is more than amount of replicas for VDisk with id {}",
                bad_vdisk.id
            ));
        }
        if node.backend_result().is_ok()
            && node.backend_type() == BackendType::Pearl
            && node.pearl().alien_disk().is_some()
        {
            let alien_disk = node.pearl().alien_disk().unwrap();
            finded
                .disks()
                .iter()
                .find(|d| alien_disk == d.name())
                .ok_or_else(|| {
                    let msg = format!(
                        "cannot find disk {:?} for node {:?} in cluster config",
                        alien_disk,
                        node.name()
                    );
                    debug!("{}", msg);
                    msg
                })?;
        }
        node.prepare(finded)
    }

    pub fn get_from_string(file: &str) -> Result<Self, String> {
        let config = YamlBobConfig::parse::<Self>(file)?;
        debug!("config: {:?}", config);
        if let Err(e) = config.validate() {
            debug!("Cluster config is not valid: {}", e);
            Err(format!("Cluster config is not valid: {}", e))
        } else {
            debug!("config is valid");
            Ok(config)
        }
    }

    pub fn get_testmode(path: String, addresses: Vec<String>) -> AnyResult<Self> {
        let disks = vec![DiskPath::new("disk_0".into(), &path)];
        let mut nodes = Vec::with_capacity(addresses.len());
        let mut vdisks = Vec::with_capacity(addresses.len());
        for (i, address) in addresses.into_iter().enumerate() {
            let node = Node {
                name: format!("node_{i}"),
                address,
                disks: disks.clone()
            };
            let replica = Replica::new(node.name().to_string(), node.disks()[0].name().to_string());
            let mut vdisk = VDisk::new(i as u32);
            vdisk.push_replica(replica);
            nodes.push(node);
            vdisks.push(vdisk);
        }
        let dist_func = DistributionFunc::default();
        let config = Cluster {
            nodes,
            vdisks,
            racks: vec![],
            distribution_func: dist_func
        };

        if let Err(e) = config.validate() {
            let msg = format!("config is not valid: {e}");
            Err(anyhow!(msg))
        } else {
            Ok(config)
        }
    }

    pub fn get_testmode_node_config(&self, n_node: usize, rest_port: Option<u16>) -> AnyResult<NodeConfig> {
        let node = &self.nodes().get(n_node).ok_or(anyhow!("node with index {} not found", n_node))?;
        let config = NodeConfig::get_testmode(node.name(), node.disks()[0].name(), rest_port);
        if let Err(e) = config.validate() {
            Err(anyhow!("config is not valid: {e}"))
        } else {
            self.check(&config)
                .map_err(|e| anyhow!("node config check failed: {e}"))?;
            Ok(config)
        }
    }
}

impl Validatable for Cluster {
    fn validate(&self) -> Result<(), String> {
        if self.nodes.is_empty() {
            return Err("bob requires at least one node to start".to_owned());
        }
        if self.vdisks.is_empty() {
            return Err("bob requires at least one virtual disk to start".to_owned());
        }
        Validator::aggregate(&self.racks).map_err(|e| {
            format!("some racks in config are invalid: {}", e)
        })?;
        Validator::aggregate(&self.nodes).map_err(|e| {
            format!("some nodes in config are invalid: {}", e)
        })?;
        Validator::aggregate(&self.vdisks).map_err(|e| {
            format!("some vdisks in config are invalid: {}", e)
        })?;

        let mut vdisk_ids = self.vdisks.iter().map(|vdisk| vdisk.id).collect::<Vec<_>>();
        vdisk_ids.sort();
        for index in 0..vdisk_ids.len() {
            if vdisk_ids[index] < index as u32 {
                return Err(format!("config contains duplicates vdisks ids: {}", vdisk_ids[index]));
            } else if vdisk_ids[index] > index as u32 {
                return Err(format!("config contains gap in vdisks ids before vdisk id = {}", vdisk_ids[index]));
            }
        }

        Validator::validate_no_duplicates(self.nodes.iter().map(|node| &node.name)).map_err(|dup_item| {
            format!("config contains duplicates nodes names: {}", dup_item)
        })?;

        Validator::validate_no_duplicates(self.nodes.iter().map(|node| &node.address)).map_err(|dup_item| {
            format!("config contains duplicates nodes addresses: {}", dup_item)
        })?;

        Validator::validate_no_duplicates(self.racks.iter().map(|rack| rack.name())).map_err(|dup_item| {
            format!("config contains duplicates racks names: {}", dup_item)
        })?;

        Validator::validate_no_duplicates(self.racks.iter().flat_map(|rack| rack.nodes())).map_err(|dup_item| {
            format!("config contains duplicate node names in racks: {}", dup_item)
        })?;

        let node_names = self.nodes.iter().map(|node| &node.name).collect::<HashSet<_>>();
        for name in self.racks.iter().flat_map(Rack::nodes) {
            if !node_names.contains(&name) {
                return Err(format!("config contains unknown node names in racks: {}", name));
            }
        }

        for vdisk in &self.vdisks {
            for replica in &vdisk.replicas {
                if let Some(node) = self.nodes.iter().find(|x| x.name == replica.node) {
                    if node.disks.iter().all(|x| *x.name() != replica.disk) {
                        return Err(format!("cannot find in node: {:?}, disk with name: {:?} for vdisk: {:?}", replica.node, replica.disk, vdisk.id));
                    }
                } else {
                    return Err(format!("cannot find node: {:?} for vdisk: {:?}", replica.node, vdisk.id));
                }
            }
        }

        Ok(())
    }
}

pub mod tests {
    use super::{Cluster, DiskPath, DistributionFunc, Node, Replica, VDisk};

    #[must_use]
    pub fn cluster_config(count_nodes: u32, count_vdisks: u32, count_replicas: u32) -> Cluster {
        let nodes = (0..count_nodes)
            .map(|id| {
                let name = id.to_string();
                Node {
                    name: name.clone(),
                    address: format!("0.0.0.0:{id}"),
                    disks: vec![DiskPath::new(name.as_str().into(), name.as_str())],
                }
            })
            .collect();

        let vdisks = (0..count_vdisks)
            .map(|id| {
                let replicas = (0..count_replicas)
                    .map(|r| {
                        let n = ((id + r) % count_nodes).to_string();
                        Replica {
                            node: n.clone(),
                            disk: n,
                        }
                    })
                    .collect();
                VDisk { id, replicas }
            })
            .collect();

        Cluster {
            nodes,
            vdisks,
            distribution_func: DistributionFunc::default(),
            racks: vec![],
        }
    }
}
