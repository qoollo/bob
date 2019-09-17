use crate::api::grpc::{BlobMeta, PutOptions, GetOptions};
use crate::core::{
    bob_client::{BobClient, BobClientFactory},
    configs::cluster::Node as ConfigNode,
};
use std::sync::{Arc, Mutex};

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

#[derive(Clone, Debug)]
pub struct BobData {
    pub data: Vec<u8>,
    pub meta: BobMeta,
}

impl BobData {
    pub fn new(data: Vec<u8>, meta: BobMeta) -> Self {
        BobData { data, meta }
    }
}

#[derive(Debug, Clone)]
pub struct BobMeta {
    pub timestamp: u32,
}
impl BobMeta {
    pub fn new(data: BlobMeta) -> Self {
        BobMeta {
            timestamp: data.timestamp,
        }
    }

    pub fn new_value(timestamp: u32) -> Self {
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
        BobOptions {flags, remote_nodes}
    }

    pub(crate) fn new_get(options: Option<GetOptions>) -> Self {
        let mut flags: BobFlags = Default::default();
        if let Some(vopts) = options {
            if vopts.force_node {
                flags |= BobFlags::FORCE_NODE;
            }
        }
        BobOptions {flags, remote_nodes: vec![]}
    }

    pub(crate) fn have_remote_node(&self) -> bool {
        self.remote_nodes.len() > 0 
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
            if self
                .replicas
                .iter()
                .find(|r| r.node_name == node.name)
                .is_some()
            {
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

#[derive(Debug, PartialEq, Clone)]
pub struct DiskPath {
    pub name: String,
    pub path: String,
}

impl DiskPath {
    pub fn new(name: &str, path: &str) -> DiskPath {
        DiskPath {
            name: name.to_string().clone(),
            path: path.to_string().clone(),
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

pub fn print_vec<T: std::fmt::Display>(coll: &[T]) -> String {
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
    pub fn get_uri(&self) -> http::Uri {
        format!("http://{}:{}", self.host, self.port)
            .parse()
            .unwrap()
    }

    pub(crate) fn counter_display(&self) -> String {
        format!("{}:{}", self.host.replace(".", "_"), self.port)
    }

    pub(crate) fn set_connection(&self, client: BobClient) {
        *self.conn.lock().unwrap() = Some(client);
    }

    pub(crate) fn clear_connection(&self) {
        *self.conn.lock().unwrap() = None;
    }

    pub(crate) fn get_connection(&self) -> Option<BobClient> {
        self.conn.lock().unwrap().clone()
    }

    pub(crate) async fn check(self, client_fatory: BobClientFactory) -> Result<(), ()> {
        match self.get_connection() {
            Some(mut conn) => {
                conn.ping()
                    .await
                    .map(|_| debug!("All good with pinging node {:?}", self))
                    .map_err(|_| {
                        debug!("Got broken connection to node {:?}", self);
                        self.clear_connection();
                    })?;
                Ok(())
            }
            None => {
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
}

impl From<&ConfigNode> for Node {
    fn from(node: &ConfigNode) -> Self {
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

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}={}:{}", self.name, self.host, self.port)
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Node) -> bool {
        self.host == other.host && self.port == other.port
    }
}

impl Eq for Node {}
