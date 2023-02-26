use crate::{
    node::{Node, NodeName},
};
use std::{
    fmt::Debug,
    hash::{Hash, Hasher},
};

pub type VDiskId = u32;

#[derive(Debug)]
pub struct VDisk {
    id: VDiskId,
    replicas: Vec<NodeDisk>,
    nodes: Vec<Node>,
}

/// Structure represents disk on the node. Contains path to disk and name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DiskPath {
    name: String,
    path: String,
}

#[derive(Debug, Clone)]
pub struct NodeDisk {
    node_name: NodeName,
    disk_path: String,
    disk_name: String,
}


impl VDisk {
    fn check_no_duplicates<TItem: Eq + Hash>(data: &[TItem]) -> bool {
        return data.len() == data.iter().collect::<std::collections::HashSet<_>>().len();
    }

    pub fn new(id: VDiskId, replicas: Vec<NodeDisk>, nodes: Vec<Node>) -> Self {
        debug_assert!(Self::check_no_duplicates(replicas.as_slice()));
        debug_assert!(Self::check_no_duplicates(nodes.as_slice()));

        VDisk {
            id,
            replicas,
            nodes,
        }
    }

    pub fn id(&self) -> VDiskId {
        self.id
    }

    pub fn replicas(&self) -> &[NodeDisk] {
        &self.replicas
    }

    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }
}

impl DiskPath {
    /// Creates new `DiskPath` with disk's name and path.
    #[must_use = "memory allocation"]
    pub fn new(name: String, path: String) -> DiskPath {
        DiskPath { name, path }
    }

    /// Returns disk name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

impl NodeDisk {
    pub fn new(disk_path: String, disk_name: String, node_name: NodeName) -> Self {
        Self {
            node_name,
            disk_path,
            disk_name,
        }
    }

    pub fn disk_path(&self) -> &str {
        &self.disk_path
    }

    pub fn disk_name(&self) -> &str {
        &self.disk_name
    }

    pub fn node_name(&self) -> &NodeName {
        &self.node_name
    }
}

impl PartialEq for NodeDisk {
    fn eq(&self, other: &NodeDisk) -> bool {
        self.node_name == other.node_name && self.disk_name == other.disk_name
    }
}

impl Eq for NodeDisk { }

impl Hash for NodeDisk {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.node_name.hash(state);
        self.disk_name.hash(state);
    }
}


// @TODO maybe merge NodeDisk and DiskPath
impl From<&NodeDisk> for DiskPath {
    fn from(node: &NodeDisk) -> Self {
        DiskPath {
            name: node.disk_name().to_owned(),
            path: node.disk_path().to_owned(),
        }
    }
}
