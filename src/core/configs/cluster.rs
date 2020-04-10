use super::prelude::*;

// @TODO move deps
use http::Uri;
use std::cell::Ref;

/// Structure represents disk on the node. Contains path to disk and name.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeDisk {
    path: String,
    name: String,
}

impl NodeDisk {
    /// Returns disk name, empty if name wasn't set in config.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns disk path, empty if path wasn't set in config.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }
}

impl Validatable for NodeDisk {
    fn validate(&self) -> Result<(), String> {
        // For some reason serde yaml deserializes "field: # no value" into '~'
        if self.name.is_empty() || self.path.is_empty() || self.name == "~" || self.path == "~" {
            let msg = format!("node disks must contain not empty fields 'name' and 'path'");
            error!("NodeDisk validation failed: {}", msg);
            Err(msg)
        } else {
            debug!("{:?} is valid", self);
            Ok(())
        }
    }
}

/// Node config struct, with name, address and [`NodeDisk`]s.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Node {
    name: String,
    address: String,
    disks: Vec<NodeDisk>,

    #[serde(skip)]
    uri: RefCell<Uri>,
}

impl Node {
    /// Returns node name, empty if name wasn't set in config.
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns slice of disk configs [`NodeDisk`]
    #[inline]
    pub fn disks(&self) -> &[NodeDisk] {
        &self.disks
    }

    /// Returns node address, empty if address wasn't set in config.
    #[inline]
    pub fn address(&self) -> &str {
        &self.address
    }

    #[inline]
    pub(crate) fn uri(&self) -> Ref<Uri> {
        self.uri.borrow()
    }

    fn prepare(&self) -> Result<(), String> {
        if let Ok(uri) = self.address().parse() {
            *self.uri.borrow_mut() = uri;
            Ok(())
        } else {
            let msg = format!("invalid, node[{}] address: {}", self.name(), self.address());
            error!("{}", msg);
            Err(msg)
        }
    }
}
impl Validatable for Node {
    fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() || self.name == "~" {
            let msg = format!("node must contain not empty field 'name'");
            error!("{}", msg);
            return Err(msg);
        }

        match self.address.parse::<Uri>() {
            Ok(uri) => {
                if uri.port_u16().is_none() {
                    let msg = format!("{}: node uri missing port", uri);
                    error!("{}", msg);
                    return Err(msg);
                }
            }
            Err(e) => {
                let msg = format!("{}: node uri parse failed: {}", self.address, e);
                error!("{}", msg);
                return Err(msg);
            }
        }

        Self::aggregate(&self.disks)?;

        let mut names = self
            .disks
            .iter()
            .map(|disk| disk.name())
            .collect::<Vec<_>>();
        names.sort();
        if names.windows(2).any(|pair| pair[0] == pair[1]) {
            let msg = format!("nodes can't use identical names: {}", self.name());
            error!("{}", msg);
            Err(msg)
        } else {
            Ok(())
        }
    }
}

/// Struct represents replica info for virtual disk.
#[derive(Debug, PartialEq, Serialize, Deserialize, Eq, PartialOrd, Ord, Clone)]
pub struct Replica {
    node: String,
    disk: String,
}

impl Replica {
    /// Creates new replica struct with given node name and disk name.
    pub fn new(node: String, disk: String) -> Self {
        Self { node, disk }
    }

    /// Returns node name, empty if name wasn't set in config.
    #[must_use]
    pub fn node(&self) -> &str {
        &self.node
    }

    #[must_use]
    fn disk(&self) -> &str {
        &self.disk
    }
}

impl Validatable for Replica {
    fn validate(&self) -> Result<(), String> {
        if self.node.is_empty() || self.disk.is_empty() {
            let msg = format!("replica must contain not empty fields 'node' and 'disk'");
            error!("{}", msg);
            Err(msg)
        } else {
            Ok(())
        }
    }
}

/// Config for virtual disks, stores replicas locations.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct VDisk {
    id: u32,
    #[serde(default)]
    replicas: Vec<Replica>,
}

impl VDisk {
    /// Creates new instance of the [`VDisk`] with given id. To add replicas use [`push_replicas`].
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
            debug!("vdisk must have replicas: {}", self.id());
            return Err(format!("vdisk must have replicas: {}", self.id()));
        }
        Self::aggregate(&self.replicas).map_err(|e| {
            debug!("vdisk is invalid: {}", self.id());
            e
        })?;

        let mut replicas_ref = self.replicas.iter().collect::<Vec<_>>();
        replicas_ref.sort();
        if replicas_ref.windows(2).any(|pair| pair[0] == pair[1]) {
            debug!("vdisk: {} contains duplicate replicas", self.id());
            Err(format!("vdisk: {} contains duplicate replicas", self.id()))
        } else {
            Ok(())
        }
    }
}

/// Config with cluster structure description.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    nodes: Vec<Node>,
    #[serde(default)]
    vdisks: Vec<VDisk>,
}

impl Config {
    /// Returns slice with [`Node`]s.
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    /// Returns slice with [`VDisk`]s.
    pub fn vdisks(&self) -> &[VDisk] {
        &self.vdisks
    }

    /// Extends the vdisks collection with contents of the iterator.
    pub fn vdisks_extend(&mut self, iter: impl IntoIterator<Item = VDisk>) {
        self.vdisks.extend(iter)
    }

    /// Creates [`DataVDisk`]s from config, required for mapper.
    pub fn convert(&self) -> Result<Vec<DataVDisk>, String> {
        let mut result = Vec::new();
        for vdisk in &self.vdisks {
            let mut disk = DataVDisk::new(vdisk.id());
            for replica in vdisk.replicas() {
                let disk_name = replica.disk().to_string();
                let node: &Node = self
                    .nodes
                    .iter()
                    .find(|node| node.name() == replica.node())
                    .expect("unknown node name in replica");
                let node_name = replica.node().to_owned();
                let disk_path = node
                    .disks()
                    .iter()
                    .find(|disk| disk.name() == disk_name)
                    .unwrap()
                    .path()
                    .to_owned();
                let node_disk = DataNodeDisk::new(disk_path, disk_name, node_name);
                disk.push_replica(node_disk);
            }
            result.push(disk);
        }
        Ok(result)
    }

    /// Loads config from disk, and validates it.
    pub fn try_get(filename: &str) -> Result<Self, String> {
        let config = YamlBobConfigReader::get::<Config>(filename)?;
        match config.validate() {
            Ok(_) => Ok(config),
            Err(e) => {
                debug!("config is not valid: {}", e);
                Err(format!("config is not valid: {}", e))
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn get_from_string(file: &str) -> Result<Self, String> {
        let config = YamlBobConfigReader::parse::<Self>(file)?;
        debug!("config: {:?}", config);
        if let Err(e) = config.validate() {
            debug!("config is not valid: {}", e);
            Err(format!("config is not valid: {}", e))
        } else {
            debug!("config is valid");
            Ok(config)
        }
    }
}

impl Validatable for Config {
    fn validate(&self) -> Result<(), String> {
        if self.nodes.is_empty() {
            let msg = format!("bob requires at least one node to start");
            error!("{}", msg);
            return Err(msg);
        }
        if self.vdisks.is_empty() {
            let msg = format!("bob requires at least one virtual disk to start");
            error!("{}", msg);
            return Err(msg);
        }
        Self::aggregate(&self.nodes).map_err(|e| {
            error!("some nodes in config are invalid");
            e
        })?;
        Self::aggregate(&self.vdisks).map_err(|e| {
            error!("some vdisks in config are invalid");
            e
        })?;

        let mut vdisks_id = self.vdisks.iter().map(|vdisk| vdisk.id).collect::<Vec<_>>();
        vdisks_id.sort();
        if vdisks_id.windows(2).any(|pair| pair[0] == pair[1]) {
            debug!("config contains duplicates vdisks ids");
            return Err("config contains duplicates vdisks ids".to_string());
        }

        let mut node_names = self.nodes.iter().map(|node| &node.name).collect::<Vec<_>>();
        node_names.sort();
        if node_names.windows(2).any(|pair| pair[0] == pair[1]) {
            debug!("config contains duplicates nodes names");
            return Err("config contains duplicates nodes names".to_string());
        }

        let err = self
            .nodes
            .iter()
            .filter_map(|x| x.prepare().err())
            .fold(String::new(), |acc, x| acc + "\n" + &x);
        if !err.is_empty() {
            return Err(err);
        }

        for vdisk in &self.vdisks {
            for replica in &vdisk.replicas {
                if let Some(node) = self.nodes.iter().find(|x| x.name == replica.node) {
                    if node.disks.iter().all(|x| x.name != replica.disk) {
                        let msg = format!(
                            "cannot find in node: {:?}, disk with name: {:?} for vdisk: {:?}",
                            replica.node, replica.disk, vdisk.id
                        );
                        error!("{}", msg);
                        return Err(msg);
                    }
                } else {
                    let msg = format!(
                        "cannot find node: {:?} for vdisk: {:?}",
                        replica.node, vdisk.id
                    );
                    error!("{}", msg);
                    return Err(msg);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[must_use]
    pub(crate) fn cluster_config(
        count_nodes: u32,
        count_vdisks: u32,
        count_replicas: u32,
    ) -> Config {
        let nodes = (0..count_nodes)
            .map(|id| {
                let name = id.to_string();
                let node = Node {
                    name: name.clone(),
                    address: "0.0.0.0:0".to_string(),
                    disks: vec![NodeDisk {
                        name: name.clone(),
                        path: name,
                    }],
                    uri: RefCell::new(Uri::default()),
                };
                node.prepare().unwrap();
                node
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

        Config { nodes, vdisks }
    }
}
