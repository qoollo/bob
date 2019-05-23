use crate::api::grpc::BlobMeta;
use crate::core::backend::BackendOperation;
use crate::core::configs::node::DiskPath as ConfigDiskPath;
use crate::core::configs::node::NodeConfig;

#[derive(Debug)]
pub enum BobError {
    Timeout,
    NotFound,
    Other(String),
}

#[derive(Debug)]
pub struct ClusterResult<T> {
    pub node: Node,
    pub result: T,
}

#[derive(Debug)]
pub struct BobPutResult {}

pub struct BobGetResult {
    pub data: BobData,
}

#[derive(Debug)]
pub struct BobPingResult {
    pub node: Node,
}

#[derive(Clone)]
pub struct BobData {
    pub data: Vec<u8>,
    pub meta: BobMeta,
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

impl std::fmt::Display for BobKey {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.key)
    }
}

bitflags! {
    #[derive(Default)]
    pub struct BobOptions: u8 {
        const FORCE_NODE = 0x01;
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
#[derive(Debug, Clone)]
pub struct VDiskMapper {
    local_node_name: String,
    disks: Vec<DiskPath>,
    vdisks: Vec<VDisk>,
}

impl VDiskMapper {
    pub fn new(vdisks: Vec<VDisk>, config: &NodeConfig) -> VDiskMapper {
        VDiskMapper {
            vdisks,
            local_node_name: config.name.as_ref().unwrap().to_string(),
            disks: config
                .disks()
                .iter()
                .map(|d| DiskPath::new(&d.name.clone(), &d.path.clone()))
                .collect(),
        }
    }
    pub fn new2(vdisks: Vec<VDisk>, node_name: &str, disks: &Vec<ConfigDiskPath>) -> VDiskMapper {
        VDiskMapper {
            vdisks,
            local_node_name: node_name.to_string(),
            disks: disks
                .iter()
                .map(|d| DiskPath::new(&d.name.clone(), &d.path.clone()))
                .collect(),
        }
    }
    pub fn vdisks_count(&self) -> u32 {
        self.vdisks.len() as u32
    }

    pub fn local_disks(&self) -> &Vec<DiskPath> {
        &self.disks
    }

    pub fn nodes(&self) -> Vec<Node> {
        let mut nodes: Vec<Node> = self
            .vdisks
            .to_vec()
            .iter()
            .flat_map(|vdisk| vdisk.replicas.iter().map(|nd| nd.node.clone()))
            .collect();
        nodes.dedup();
        nodes
    }

    pub fn get_vdisk(&self, key: BobKey) -> &VDisk {
        let vdisk_id = VDiskId::new((key.key % self.vdisks.len() as u64) as u32);
        self.vdisks.iter().find(|disk| disk.id == vdisk_id).unwrap()
    }

    pub fn get_vdisks_by_disk(&self, disk: &str) -> Vec<VDiskId> {
        self.vdisks
            .iter()
            .filter(|vdisk| {
                vdisk.replicas.iter().any(|replica| {
                    replica.node.name == self.local_node_name && replica.name == disk
                })
            })
            .map(|vdisk| vdisk.id.clone())
            .collect()
    }

    pub fn get_operation(&self, key: BobKey) -> BackendOperation {
        let vdisk_id = VDiskId::new((key.key % self.vdisks.len() as u64) as u32);
        let vdisk = self.vdisks.iter().find(|disk| disk.id == vdisk_id).unwrap();
        let disk = vdisk
            .replicas
            .iter()
            .find(|disk| disk.node.name == self.local_node_name);
        if disk.is_none() {
            trace!(
                "cannot find node: {} for vdisk: {}",
                self.local_node_name,
                vdisk_id
            );
            return BackendOperation::new_other(vdisk_id);
        }
        BackendOperation::new_local(
            vdisk_id,
            DiskPath::new(&disk.unwrap().name, &disk.unwrap().path),
        )
    }
}

#[derive(Clone, Eq)]
pub struct Node {
    pub name: String,
    pub host: String,
    pub port: u16,
}

impl Node {
    pub fn get_uri(&self) -> http::Uri {
        format!("http://{}:{}", self.host, self.port)
            .parse()
            .unwrap()
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

#[derive(Debug, Clone)]
pub struct NodeDisk {
    pub node: Node,
    pub path: String,
    pub name: String,
}

impl std::fmt::Display for NodeDisk {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}/{}-{}", self.node, self.name, self.path)
    }
}

impl PartialEq for NodeDisk {
    fn eq(&self, other: &NodeDisk) -> bool {
        self.node == other.node && self.path == other.path && self.name == other.name
    }
}

pub fn print_vec<T: std::fmt::Display>(coll: &[T]) -> String {
    coll.iter()
        .map(|vd| vd.to_string())
        .collect::<Vec<_>>()
        .join(",")
}
