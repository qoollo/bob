use super::prelude::*;

impl PutOptions {
    pub(crate) fn new_client() -> Self {
        PutOptions {
            remote_nodes: vec![],
            force_node: true,
            overwrite: false,
        }
    }

    pub(crate) fn new_alien(nodes: &[String]) -> Self {
        PutOptions {
            remote_nodes: nodes.to_vec(),
            force_node: true,
            overwrite: false,
        }
    }
}

impl GetOptions {
    pub(crate) fn new_normal() -> Self {
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
                panic!("cannot convert value: {} to 'GetSource' enum", other);
            }
        }
    }
}

#[derive(Debug)]
pub struct ClusterResult<T> {
    pub node: Node,
    pub result: T,
}

impl<T: std::fmt::Display> std::fmt::Display for ClusterResult<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "node: {}, result: {}", self.node, self.result)
    }
}

#[derive(Clone)]
pub struct BobData {
    pub data: Vec<u8>,
    pub meta: BobMeta,
}

impl BobData {
    pub fn new(data: Vec<u8>, meta: BobMeta) -> Self {
        BobData { data, meta }
    }
}

impl std::fmt::Display for BobData {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "(len: {}, meta: {})", self.data.len(), self.meta)
    }
}

impl std::fmt::Debug for BobData {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "(len: {}, meta: {})", self.data.len(), self.meta)
    }
}

#[derive(Debug, Clone)]
pub struct BobMeta {
    pub timestamp: i64,
}
impl BobMeta {
    pub fn new(data: BlobMeta) -> Self {
        BobMeta {
            timestamp: data.timestamp,
        }
    }

    pub fn new_value(timestamp: i64) -> Self {
        BobMeta { timestamp }
    }

    pub fn new_stub() -> Self {
        BobMeta { timestamp: 1 }
    }
}

impl std::fmt::Display for BobMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.timestamp)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct BobKey {
    pub key: u64,
}

impl BobKey {
    pub fn new(key: u64) -> Self {
        BobKey { key }
    }
}

impl std::fmt::Display for BobKey {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.key)
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
    pub flags: BobFlags,
    pub remote_nodes: Vec<String>,
    get_source: Option<GetSource>,
}

impl BobOptions {
    pub(crate) fn new_put(options: Option<PutOptions>) -> Self {
        let mut flags: BobFlags = Default::default();
        let mut remote_nodes = vec![];
        if let Some(vopts) = options {
            if vopts.force_node {
                flags |= BobFlags::FORCE_NODE;
            }
            remote_nodes = vopts.remote_nodes;
        }
        BobOptions {
            flags,
            remote_nodes,
            get_source: None,
        }
    }

    pub(crate) fn new_get(options: Option<GetOptions>) -> Self {
        let mut flags: BobFlags = Default::default();
        let mut get_source = None;
        if let Some(vopts) = options {
            if vopts.force_node {
                flags |= BobFlags::FORCE_NODE;
            }
            get_source = Some(GetSource::from(vopts.source));
        }
        BobOptions {
            flags,
            remote_nodes: vec![],
            get_source,
        }
    }

    #[inline]
    pub(crate) fn have_remote_node(&self) -> bool {
        !self.remote_nodes.is_empty()
    }

    pub(crate) fn get_normal(&self) -> bool {
        if let Some(value) = self.get_source {
            if value == GetSource::All || value == GetSource::Normal {
                return true;
            }
        }
        false
    }

    pub(crate) fn get_alien(&self) -> bool {
        if let Some(value) = self.get_source {
            if value == GetSource::All || value == GetSource::Alien {
                return true;
            }
        }
        false
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VDiskId {
    id: u32,
}

impl VDiskId {
    pub fn new(id: u32) -> VDiskId {
        VDiskId { id }
    }

    pub fn as_u32(&self) -> u32 {
        self.id
    }
}

impl std::fmt::Display for VDiskId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

#[derive(Debug, Clone)]
pub struct VDisk {
    pub id: VDiskId,
    pub replicas: Vec<NodeDisk>,
    pub nodes: Vec<Node>,
}

impl VDisk {
    pub(crate) fn new(id: VDiskId, capacity: usize) -> Self {
        VDisk {
            id,
            replicas: Vec::with_capacity(capacity),
            nodes: vec![],
        }
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

impl std::fmt::Display for VDisk {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "#{}-{}",
            self.id,
            self.replicas
                .iter()
                .map(|nd| nd.to_string())
                .collect::<Vec<_>>()
                .join(",")
        )
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct DiskPath {
    pub name: String,
    pub path: String,
}

impl DiskPath {
    pub fn new(name: &str, path: &str) -> DiskPath {
        DiskPath {
            name: name.to_string(),
            path: path.to_string(),
        }
    }
}

impl std::fmt::Display for DiskPath {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "#{}-{}", self.name, self.path)
    }
}
impl From<&NodeDisk> for DiskPath {
    fn from(node: &NodeDisk) -> Self {
        DiskPath {
            name: node.disk_name.clone(),
            path: node.disk_path.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NodeDisk {
    pub disk_path: String,
    pub disk_name: String,
    pub node_name: String,
}

impl std::fmt::Display for NodeDisk {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}-{}:{}",
            self.node_name, self.disk_name, self.disk_path
        )
    }
}

impl PartialEq for NodeDisk {
    fn eq(&self, other: &NodeDisk) -> bool {
        self.node_name == other.node_name && self.disk_name == other.disk_name
    }
}

pub fn print_vec<T: Display>(coll: &[T]) -> String {
    coll.iter()
        .map(|vd| vd.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

#[derive(Clone)]
pub struct Node {
    pub name: String,
    pub host: String,
    pub port: u16,

    pub index: u16,
    conn: Arc<Mutex<Option<BobClient>>>,
}

impl Node {
    pub fn name(&self) -> String {
        self.name.clone()
    }
    pub fn new(name: &str, host: &str, port: u16) -> Self {
        Node {
            name: name.to_string(),
            host: host.to_string(),
            port,
            index: 0,
            conn: Arc::new(Mutex::new(None)),
        }
    }

    pub fn get_address(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }

    pub fn get_uri(&self) -> http::Uri {
        self.get_address().parse().expect("parse uri")
    }

    pub(crate) fn counter_display(&self) -> String {
        format!("{}:{}", self.host.replace(".", "_"), self.port)
    }

    pub(crate) fn set_connection(&self, client: BobClient) {
        *self.conn.lock().expect("lock mutex") = Some(client);
    }

    pub(crate) fn clear_connection(&self) {
        debug!("clear connection");
        *self.conn.lock().expect("lock mutex") = None;
    }

    pub(crate) fn get_connection(&self) -> Option<BobClient> {
        self.conn.lock().expect("lock mutex").clone()
    }

    pub(crate) async fn check(self, client_fatory: Factory) -> Result<(), String> {
        let connection = self.get_connection();
        if let Some(mut conn) = connection {
            conn.ping()
                .await
                .map(|_| debug!("All good with pinging node {:?}", self))
                .map_err(|e| {
                    debug!("Got broken connection to node {:?}", self);
                    self.clear_connection();
                    e.to_string()
                })
        } else {
            debug!("will connect to {:?}", self);
            client_fatory
                .produce(self.clone())
                .await
                .map(move |client| {
                    self.set_connection(client);
                })
        }
    }
}

impl From<&ClusterNodeConfig> for Node {
    fn from(node: &ClusterNodeConfig) -> Self {
        Node::new(&node.name(), &node.host(), node.port())
    }
}
impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}={}:{}", self.name, self.host, self.port)
    }
}

impl std::hash::Hash for Node {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.host.hash(state);
        self.port.hash(state);
    }
}

impl Debug for Node {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}={}:{}", self.name, self.host, self.port)
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Node) -> bool {
        self.host == other.host && self.port == other.port
    }
}

impl Eq for Node {}
