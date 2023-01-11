use crate::{
    error::Error,
    mapper::NodesMap,
    node::{Disk as NodeDisk, Node},
};
use bob_grpc::{DeleteOptions, GetOptions, GetSource, PutOptions};
use bytes::{Bytes, BytesMut};
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

impl From<BobKey> for Vec<u8> {
    fn from(val: BobKey) -> Self {
        val.iter().cloned().collect()
    }
}

impl From<BobKey> for [u8; BOB_KEY_SIZE] {
    fn from(val: BobKey) -> Self {
        val.0
    }
}

impl BobKey {
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &u8> {
        self.0.iter()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
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
    inner: Bytes,
    meta: BobMeta,
}

impl BobData {
    const TIMESTAMP_LEN: usize = 8;

    pub fn new(inner: Bytes, meta: BobMeta) -> Self {
        BobData { inner, meta }
    }

    pub fn inner(&self) -> &[u8] {
        &self.inner
    }

    pub fn into_inner(self) -> Bytes {
        self.inner
    }

    pub fn meta(&self) -> &BobMeta {
        &self.meta
    }

    pub fn to_serialized_bytes(&self) -> Bytes {
        let mut result = BytesMut::with_capacity(Self::TIMESTAMP_LEN + self.inner.len());
        result.extend_from_slice(&self.meta.timestamp.to_be_bytes());
        result.extend_from_slice(&self.inner);
        result.freeze()
    }

    pub fn from_serialized_bytes(mut bob_data: Bytes) -> Result<BobData, Error> {
        let ts_bytes = bob_data.split_to(Self::TIMESTAMP_LEN);
        let ts_bytes = (&*ts_bytes)
            .try_into()
            .map_err(|e| Error::storage(format!("parse error: {}", e)))?;
        let timestamp = u64::from_be_bytes(ts_bytes);
        let meta = BobMeta::new(timestamp);
        Ok(BobData::new(bob_data, meta))
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


#[derive(Debug)]
pub struct BobPutOptions {
    force_node: bool,
    remote_nodes: Vec<String>
}

#[derive(Debug)]
pub struct BobGetOptions {
    force_node: bool,
    get_source: Option<GetSource>,
}

#[derive(Debug)]
pub struct BobDeleteOptions {
    force_node: bool,
    is_alien: bool,
    force_alien_nodes: Vec<String>
}

impl BobPutOptions {
    pub fn new_put(options: Option<PutOptions>) -> Self {
        if let Some(vopts) = options {
            BobPutOptions {
                force_node: vopts.force_node,
                remote_nodes: vopts.remote_nodes
            }
        } else {
            BobPutOptions {
                force_node: false,
                remote_nodes: vec![]
            }
        }
    }

    pub fn force_node(&self) -> bool {
        self.force_node
    }

    pub fn to_alien(&self) -> bool {
        !self.remote_nodes.is_empty()
    }

    pub fn remote_nodes(&self) -> &[String] {
        &self.remote_nodes
    }
}

impl BobGetOptions {
    pub fn new_get(options: Option<GetOptions>) -> Self {
        if let Some(vopts) = options {
            BobGetOptions {
                force_node: vopts.force_node,
                get_source: Some(GetSource::from(vopts.source))
            }
        } else {
            BobGetOptions {
                force_node: false,
                get_source: None
            }
        }
    }

    pub fn force_node(&self) -> bool {
        self.force_node
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


impl BobDeleteOptions {
    pub fn new_delete(options: Option<DeleteOptions>) -> Self {
        if let Some(vopts) = options {
            BobDeleteOptions {
                force_node: vopts.force_node,
                is_alien: vopts.is_alien,
                force_alien_nodes: vopts.force_alien_nodes
            }
        } else {
            BobDeleteOptions {
                force_node: false,
                is_alien: false,
                force_alien_nodes: vec![]
            }
        }
    }

    pub fn force_node(&self) -> bool {
        self.force_node
    }

    pub fn to_alien(&self) -> bool {
        self.is_alien
    }

    pub fn force_delete_nodes(&self) -> &[String] {
        &self.force_alien_nodes
    }

    pub fn is_force_delete(&self, node_name: &str) -> bool {
        self.force_alien_nodes.iter().any(|x| x == node_name)
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
