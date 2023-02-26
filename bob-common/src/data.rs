use crate::{
    error::Error,
    node::{Disk as NodeDisk, Node, NodeName},
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


#[derive(Debug, Clone)]
pub struct BobPutOptions {
    force_node: bool,
    overwrite: bool,
    remote_nodes: Vec<NodeName>
}

#[derive(Debug, Clone)]
pub struct BobGetOptions {
    force_node: bool,
    get_source: GetSource,
}

#[derive(Debug, Clone)]
pub struct BobDeleteOptions {
    force_node: bool,
    is_alien: bool,
    force_alien_nodes: Vec<NodeName>
}

impl BobPutOptions {
    pub fn new_local() -> Self {
        BobPutOptions {
            remote_nodes: vec![],
            force_node: true,
            overwrite: false,
        }
    }

    pub fn new_alien(remote_nodes: Vec<NodeName>) -> Self {
        BobPutOptions {
            remote_nodes,
            force_node: true,
            overwrite: false,
        }
    }

    pub fn from_grpc(options: Option<PutOptions>) -> Self {
        if let Some(vopts) = options {
            BobPutOptions {
                force_node: vopts.force_node,
                overwrite: vopts.overwrite,
                remote_nodes: vopts.remote_nodes.iter().map(|nn| NodeName::from(nn)).collect()
            }
        } else {
            BobPutOptions {
                force_node: false,
                overwrite: false,
                remote_nodes: vec![]
            }
        }
    }

    pub fn to_grpc(&self) -> PutOptions {
        PutOptions { 
            remote_nodes: self.remote_nodes.iter().map(|nn| nn.to_string()).collect(), 
            force_node: self.force_node, 
            overwrite: self.overwrite 
        }
    }

    pub fn force_node(&self) -> bool {
        self.force_node
    }

    pub fn overwrite(&self) -> bool {
        self.overwrite
    }

    pub fn to_alien(&self) -> bool {
        !self.remote_nodes.is_empty()
    }

    pub fn remote_nodes(&self) -> &[NodeName] {
        &self.remote_nodes
    }
}

impl BobGetOptions {
    pub fn new_local() -> Self {
        BobGetOptions {
            force_node: true,
            get_source: GetSource::Normal,
        }
    }

    pub fn new_alien() -> Self {
        BobGetOptions {
            force_node: true,
            get_source: GetSource::Alien,
        }
    }

    pub fn new_all() -> Self {
        BobGetOptions {
            force_node: true,
            get_source: GetSource::All,
        }
    }


    pub fn from_grpc(options: Option<GetOptions>) -> Self {
        if let Some(vopts) = options {
            BobGetOptions {
                force_node: vopts.force_node,
                get_source: GetSource::from(vopts.source)
            }
        } else {
            BobGetOptions {
                force_node: false,
                get_source: GetSource::All
            }
        }
    }

    pub fn to_grpc(&self) -> GetOptions {
        GetOptions { 
            force_node: self.force_node, 
            source: self.get_source.into()
        }
    }

    pub fn force_node(&self) -> bool {
        self.force_node
    }

    pub fn get_normal(&self) -> bool {
        self.get_source == GetSource::Normal || self.get_source == GetSource::All
    }

    pub fn get_alien(&self) -> bool {
        self.get_source == GetSource::Alien || self.get_source == GetSource::All
    }
}


impl BobDeleteOptions {
    pub fn new_local() -> Self {
        BobDeleteOptions {
            force_node: true,
            is_alien: false,
            force_alien_nodes: vec![]
        }
    }

    pub fn new_alien(force_alien_nodes: Vec<NodeName>) -> Self {
        BobDeleteOptions { 
            force_node: true, 
            is_alien: true, 
            force_alien_nodes: force_alien_nodes
        }
    }

    pub fn from_grpc(options: Option<DeleteOptions>) -> Self {
        if let Some(vopts) = options {
            BobDeleteOptions {
                force_node: vopts.force_node,
                is_alien: vopts.is_alien,
                force_alien_nodes: vopts.force_alien_nodes.iter().map(|nn| NodeName::from(nn)).collect()
            }
        } else {
            BobDeleteOptions {
                force_node: false,
                is_alien: false,
                force_alien_nodes: vec![]
            }
        }
    }

    pub fn to_grpc(&self) -> DeleteOptions {
        DeleteOptions { 
            force_alien_nodes: self.force_alien_nodes.iter().map(|nn| nn.to_string()).collect(), 
            force_node: self.force_node, 
            is_alien: self.is_alien 
        }
    }

    pub fn force_node(&self) -> bool {
        self.force_node
    }

    pub fn to_alien(&self) -> bool {
        self.is_alien
    }

    pub fn force_delete_nodes(&self) -> &[NodeName] {
        &self.force_alien_nodes
    }

    pub fn is_force_delete(&self, node_name: &NodeName) -> bool {
        self.force_alien_nodes.iter().any(|x| x == node_name)
    }
}

#[derive(Debug)]
pub struct VDisk {
    id: VDiskId,
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
