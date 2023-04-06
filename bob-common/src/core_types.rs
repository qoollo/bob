pub use super::name_types::DiskName; // Re-export
use crate::{
    node::{Node, NodeName},
};
use std::{
    fmt::Debug,
    sync::Arc,
    hash::{Hash, Hasher},
};


/// Structure represents disk on the node. Contains path to disk and name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DiskPath {
    name: DiskName,
    path: Arc<str>,
}

#[derive(Debug, Clone)]
pub struct NodeDisk {
    node_name: NodeName,
    disk_name: DiskName,
    disk_path: Arc<str>,
}

pub type VDiskId = u32;

#[derive(Debug)]
pub struct VDisk {
    id: VDiskId,
    /// VDisk replicas. 
    /// Vec here is better than HashMap, because we have small number of replicas (ususally 2 or 3).
    /// Vec is outperform HashMap with small number of entries. HashMap becomes faster only with 10 elements or more according to tests.
    replicas: Vec<NodeDisk>,
    nodes: Vec<Node>,
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
    pub fn new(name: DiskName, path: &str) -> DiskPath {
        DiskPath { 
            name, 
            path: path.into()
        }
    }

    /// Returns disk name.
    #[must_use]
    pub fn name(&self) -> &DiskName {
        &self.name
    }

    pub fn path(&self) -> &str {
        self.path.as_ref()
    }
}


impl NodeDisk {
    pub fn new(disk_path: &str, disk_name: DiskName, node_name: NodeName) -> Self {
        Self {
            node_name,
            disk_name,
            disk_path: disk_path.into(),
        }
    }

    pub fn disk_path(&self) -> &str {
        self.disk_path.as_ref()
    }

    pub fn disk_name(&self) -> &DiskName {
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
            name: node.disk_name.clone(),
            path: node.disk_path.clone(),
        }
    }
}
