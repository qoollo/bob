use crate::mapper::NodesMap;

use super::prelude::*;
use std::hash::Hash;

include!(concat!(env!("OUT_DIR"), "/key_constants.rs"));

pub type VDiskID = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BobKey {
    data: [u8; KEY_SIZE],
}

impl BobKey {
    pub(crate) fn bytes(&self) -> impl Iterator<Item = &u8> {
        self.data.iter()
    }

    pub(crate) fn vdisk_hint(&self, begin: usize, end: usize) -> usize {
        let len = end - begin;
        let mut rem = 0;
        for &byte in self.bytes() {
            rem += byte as usize % len;
            rem %= len;
        }
        begin + rem
    }
}

impl std::fmt::Display for BobKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        for key in self.data.iter() {
            write!(f, "{:x}", key)?;
        }
        Ok(())
    }
}

impl std::str::FromStr for BobKey {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut data = [0; KEY_SIZE];
        for i in (0..s.len()).step_by(2) {
            if let Ok(n) = u8::from_str_radix(&s[i..i + 2], 16) {
                data[i / 2] = n
            }
        }
        Ok(Self { data })
    }
}

impl From<Vec<u8>> for BobKey {
    fn from(v: Vec<u8>) -> Self {
        let mut data = [0; KEY_SIZE];
        for (ind, elem) in v.into_iter().enumerate() {
            data[ind] = elem;
        }
        Self { data }
    }
}

impl Into<Vec<u8>> for BobKey {
    fn into(self) -> Vec<u8> {
        self.bytes().cloned().collect()
    }
}

impl PutOptions {
    pub(crate) fn new_local() -> Self {
        PutOptions {
            remote_nodes: vec![],
            force_node: true,
            overwrite: false,
        }
    }

    pub(crate) fn new_alien(remote_nodes: Vec<String>) -> Self {
        PutOptions {
            remote_nodes,
            force_node: true,
            overwrite: false,
        }
    }
}

impl GetOptions {
    pub(crate) fn new_local() -> Self {
        GetOptions {
            force_node: true,
            source: GetSource::Normal as i32,
        }
    }

    pub(crate) fn new_alien() -> Self {
        GetOptions {
            force_node: true,
            source: GetSource::Alien as i32,
        }
    }

    pub(crate) fn new_all() -> Self {
        GetOptions {
            force_node: true,
            source: GetSource::All as i32,
        }
    }
}

impl From<i32> for GetSource {
    fn from(value: i32) -> Self {
        match value {
            0 => GetSource::All,
            1 => GetSource::Normal,
            2 => GetSource::Alien,
            other => {
                error!("cannot convert value: {} to 'GetSource' enum", other);
                panic!("fatal core error");
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct BobData {
    inner: Vec<u8>,
    meta: BobMeta,
}

impl BobData {
    pub(crate) fn new(inner: Vec<u8>, meta: BobMeta) -> Self {
        BobData { inner, meta }
    }

    pub(crate) fn inner(&self) -> &[u8] {
        &self.inner
    }

    pub(crate) fn into_inner(self) -> Vec<u8> {
        self.inner
    }

    pub(crate) fn meta(&self) -> &BobMeta {
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
pub(crate) struct BobMeta {
    timestamp: u64,
}
impl BobMeta {
    pub(crate) fn new(timestamp: u64) -> Self {
        Self { timestamp }
    }

    #[inline]
    pub(crate) fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub(crate) fn stub() -> Self {
        BobMeta { timestamp: 1 }
    }
}

bitflags! {
    #[derive(Default)]
    pub(crate) struct BobFlags: u8 {
        const FORCE_NODE = 0x01;
    }
}

#[derive(Debug)]
pub(crate) struct BobOptions {
    flags: BobFlags,
    remote_nodes: Vec<String>,
    get_source: Option<GetSource>,
}

impl BobOptions {
    pub(crate) fn new_put(options: Option<PutOptions>) -> Self {
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

    pub(crate) fn new_get(options: Option<GetOptions>) -> Self {
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

    pub(crate) fn remote_nodes(&self) -> &[String] {
        &self.remote_nodes
    }

    pub(crate) fn flags(&self) -> BobFlags {
        self.flags
    }

    pub(crate) fn get_normal(&self) -> bool {
        self.get_source.map_or(false, |value| {
            value == GetSource::All || value == GetSource::Normal
        })
    }

    pub(crate) fn get_alien(&self) -> bool {
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
    pub(crate) fn new(id: VDiskID) -> Self {
        println!("key size = {}", std::mem::size_of::<BobKey>());
        VDisk {
            id,
            replicas: Vec::new(),
            nodes: Vec::new(),
        }
    }

    pub(crate) fn id(&self) -> VDiskID {
        self.id
    }

    pub(crate) fn replicas(&self) -> &[NodeDisk] {
        &self.replicas
    }

    pub(crate) fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    pub(crate) fn push_replica(&mut self, value: NodeDisk) {
        self.replicas.push(value)
    }

    pub(crate) fn set_nodes(&mut self, nodes: &NodesMap) {
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

    pub(crate) fn path(&self) -> &str {
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
