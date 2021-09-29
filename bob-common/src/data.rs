use crate::{
    mapper::NodesMap,
    node::{Disk as NodeDisk, Node},
};
use bob_grpc::{GetOptions, GetSource, PutOptions};
use std::{
    convert::TryInto,
    fmt::{Debug, Formatter, Result as FmtResult},
    hash::Hash,
};

include!(concat!(env!("OUT_DIR"), "/key_constants.rs"));

#[derive(Copy, Clone, Hash, PartialEq, Eq, Debug)]
pub struct BobKey([u8; BOB_KEY_SIZE]);

impl From<u64> for BobKey {
    fn from(n: u64) -> Self {
        let mut key = [0; BOB_KEY_SIZE];
        key.iter_mut().zip(n.to_le_bytes()).for_each(|(a, b)| {
            *a = b;
        });
        Self(key)
    }
}

impl<'a> From<&'a [u8]> for BobKey {
    fn from(a: &[u8]) -> Self {
        let data = a.try_into().expect("key size mismatch");
        Self(data)
    }
}

impl From<Vec<u8>> for BobKey {
    fn from(v: Vec<u8>) -> Self {
        let data = v.try_into().expect("key size mismatch");
        Self(data)
    }
}

impl Into<Vec<u8>> for BobKey {
    fn into(self) -> Vec<u8> {
        self.iter().cloned().collect()
    }
}

impl Into<[u8; BOB_KEY_SIZE]> for BobKey {
    fn into(self) -> [u8; BOB_KEY_SIZE] {
        self.0
    }
}

impl BobKey {
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &u8> {
        self.0.iter()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0[..]
    }
}

impl std::fmt::Display for BobKey {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        for key in self.iter() {
            write!(f, "{:02X}", key)?;
        }
        Ok(())
    }
}

impl std::str::FromStr for BobKey {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, ()> {
        let mut data = [0; BOB_KEY_SIZE];
        for i in (0..s.len()).step_by(2) {
            if let Ok(n) = u8::from_str_radix(&s[i..i + 2], 16) {
                data[i / 2] = n
            }
        }
        Ok(Self(data))
    }
}

impl Default for BobKey {
    fn default() -> Self {
        BobKey([0; BOB_KEY_SIZE])
    }
}

pub type VDiskId = u32;

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
    id: VDiskId,
    replicas: Vec<NodeDisk>,
    nodes: Vec<Node>,
}

impl VDisk {
    pub fn new(id: VDiskId) -> Self {
        VDisk {
            id,
            replicas: Vec::new(),
            nodes: Vec::new(),
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
