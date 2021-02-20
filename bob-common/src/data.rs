use crate::{
    mapper::NodesMap,
    node::{Disk as NodeDisk, Node},
};
use bob_grpc::{GetOptions, GetSource, PutOptions};
use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    hash::Hash,
};

pub type BobKey = u64;

pub type VDiskID = u32;

#[derive(Clone)]
pub struct BobData {
    inner: Vec<u8>,
    meta: BobMeta,
}

impl BobData {
    pub fn new(inner: Vec<u8>, meta: BobMeta) -> Self {
        BobData { inner, meta }
    }

    pub fn inner(&self) -> &[u8] {
        &self.inner
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.inner
    }

    pub fn meta(&self) -> &BobMeta {
        &self.meta
    }
}

impl Debug for BobData {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        f.debug_struct("BobData")
            .field("len", &self.inner.len())
            .field("meta", self.meta())
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct BobMeta {
    timestamp: u64,
}
impl BobMeta {
    pub fn new(timestamp: u64) -> Self {
        Self { timestamp }
    }

    #[inline]
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn stub() -> Self {
        BobMeta { timestamp: 1 }
    }
}

bitflags! {
    #[derive(Default)]
    pub struct BobFlags: u8 {
        const FORCE_NODE = 0x01;
    }
}

#[derive(Debug)]
pub struct BobOptions {
    flags: BobFlags,
    remote_nodes: Vec<String>,
    get_source: Option<GetSource>,
}

impl BobOptions {
    pub fn new_put(options: Option<PutOptions>) -> Self {
        let mut flags = BobFlags::default();
        let remote_nodes = options.map_or(Vec::new(), |vopts| {
            if vopts.force_node {
                flags |= BobFlags::FORCE_NODE;
            }
            vopts.remote_nodes
        });
        BobOptions {
            flags,
            remote_nodes,
            get_source: None,
        }
    }

    pub fn new_get(options: Option<GetOptions>) -> Self {
        let mut flags = BobFlags::default();

        let get_source = options.map(|vopts| {
            if vopts.force_node {
                flags |= BobFlags::FORCE_NODE;
            }
            GetSource::from(vopts.source)
        });
        BobOptions {
            flags,
            remote_nodes: Vec::new(),
            get_source,
        }
    }

    pub fn remote_nodes(&self) -> &[String] {
        &self.remote_nodes
    }

    pub fn flags(&self) -> BobFlags {
        self.flags
    }

    pub fn get_normal(&self) -> bool {
        self.get_source.map_or(false, |value| {
            value == GetSource::All || value == GetSource::Normal
        })
    }

    pub fn get_alien(&self) -> bool {
        self.get_source.map_or(false, |value| {
            value == GetSource::All || value == GetSource::Alien
        })
    }
}

#[derive(Debug, Clone)]
pub struct VDisk {
    id: VDiskID,
    replicas: Vec<NodeDisk>,
    nodes: Vec<Node>,
}

impl VDisk {
    pub fn new(id: VDiskID) -> Self {
        VDisk {
            id,
            replicas: Vec::new(),
            nodes: Vec::new(),
        }
    }

    pub fn id(&self) -> VDiskID {
        self.id
    }

    pub fn replicas(&self) -> &[NodeDisk] {
        &self.replicas
    }

    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    pub fn push_replica(&mut self, value: NodeDisk) {
        self.replicas.push(value)
    }

    pub fn set_nodes(&mut self, nodes: &NodesMap) {
        nodes.values().for_each(|node| {
            if self.replicas.iter().any(|r| r.node_name() == node.name()) {
                //TODO check if some duplicates
                self.nodes.push(node.clone());
            }
        })
    }
}

/// Structure represents disk on the node. Contains path to disk and name.
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DiskPath {
    name: String,
    path: String,
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

// @TODO maybe merge NodeDisk and DiskPath
impl From<&NodeDisk> for DiskPath {
    fn from(node: &NodeDisk) -> Self {
        DiskPath {
            name: node.disk_name().to_owned(),
            path: node.disk_path().to_owned(),
        }
    }
}
