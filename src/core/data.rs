use super::prelude::*;
use std::hash::{Hash, Hasher};

pub type BobKey = u64;

pub type VDiskId = u32;

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

#[derive(Debug)]
pub(crate) struct NodeOutput<T> {
    node_name: String,
    inner: T,
}

impl<T> NodeOutput<T> {
    pub(crate) fn new(node_name: String, inner: T) -> Self {
        Self { node_name, inner }
    }

    pub(crate) fn node_name(&self) -> &str {
        &self.node_name
    }

    pub(crate) fn inner(&self) -> &T {
        &self.inner
    }

    pub(crate) fn into_inner(self) -> T {
        self.inner
    }
}

impl NodeOutput<BobData> {
    pub(crate) fn timestamp(&self) -> u64 {
        self.inner.meta().timestamp()
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
    remote_nodes: Option<Vec<String>>,
    get_source: Option<GetSource>,
}

impl BobOptions {
    pub(crate) fn new_put(options: Option<PutOptions>) -> Self {
        let mut flags = BobFlags::default();
        let remote_nodes = options.map(|vopts| {
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
            remote_nodes: None,
            get_source,
        }
    }

    pub(crate) fn remote_nodes(&self) -> Option<&[String]> {
        self.remote_nodes.as_deref()
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
    id: VDiskId,
    replicas: Vec<NodeDisk>,
    nodes: Vec<Node>,
}

impl VDisk {
    pub(crate) fn new(id: VDiskId) -> Self {
        VDisk {
            id,
            replicas: Vec::new(),
            nodes: Vec::new(),
        }
    }

    pub(crate) fn id(&self) -> VDiskId {
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

    pub(crate) fn set_nodes(&mut self, nodes: &[Node]) {
        nodes.iter().for_each(|node| {
            if self.replicas.iter().any(|r| r.node_name == node.name) {
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
    pub(crate) fn new(name: String, path: String) -> DiskPath {
        DiskPath { name, path }
    }

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
            name: node.disk_name.clone(),
            path: node.disk_path.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct NodeDisk {
    disk_path: String,
    disk_name: String,
    node_name: String,
}

impl NodeDisk {
    pub(crate) fn new(disk_path: String, disk_name: String, node_name: String) -> Self {
        Self {
            disk_path,
            disk_name,
            node_name,
        }
    }

    pub(crate) fn disk_path(&self) -> &str {
        &self.disk_path
    }

    pub(crate) fn disk_name(&self) -> &str {
        &self.disk_name
    }

    pub(crate) fn node_name(&self) -> &str {
        &self.node_name
    }
}

impl PartialEq for NodeDisk {
    fn eq(&self, other: &NodeDisk) -> bool {
        self.node_name == other.node_name && self.disk_name == other.disk_name
    }
}

#[derive(Clone)]
pub(crate) struct Node {
    name: String,
    host: String,
    port: u16,
    index: u16,
    conn: Arc<RwLock<Option<BobClient>>>,
}

impl Node {
    pub(crate) fn new(name: String, host: String, port: u16, index: u16) -> Self {
        Self {
            name,
            host,
            port,
            index,
            conn: Arc::default(),
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn index(&self) -> u16 {
        self.index
    }

    pub(crate) fn get_address(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }

    pub(crate) fn get_uri(&self) -> Uri {
        self.get_address().parse().expect("parse uri")
    }

    pub(crate) fn counter_display(&self) -> String {
        format!("{}:{}", self.host.replace(".", "_"), self.port)
    }

    pub(crate) async fn set_connection(&self, client: BobClient) {
        debug!("acquire mutex lock on connection");
        *self.conn.write().await = Some(client);
    }

    pub(crate) async fn clear_connection(&self) {
        debug!("acquire mutex lock on connection");
        *self.conn.write().await = None;
    }

    pub(crate) async fn get_connection(&self) -> Option<BobClient> {
        debug!("acquire mutex lock on connection");
        self.conn.read().await.clone()
    }

    pub(crate) async fn check(&self, client_fatory: &Factory) -> Result<(), String> {
        if let Some(conn) = self.get_connection().await {
            if let Err(e) = conn.ping().await {
                debug!("Got broken connection to node {:?}", self);
                self.clear_connection().await;
                Err(format!("{:?}", e))
            } else {
                debug!("All good with pinging node {:?}", self);
                Ok(())
            }
        } else {
            debug!("will connect to {:?}", self);
            let client = client_fatory.produce(self.clone()).await?;
            self.set_connection(client).await;
            Ok(())
        }
    }
}

impl Hash for Node {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.host.hash(state);
        self.port.hash(state);
    }
}

impl Debug for Node {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}={}:{}", self.name, self.host, self.port)
    }
}

// @TODO get off partialeq trait
impl PartialEq for Node {
    fn eq(&self, other: &Node) -> bool {
        self.host == other.host && self.port == other.port
    }
}

impl Eq for Node {}
