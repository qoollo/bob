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

#[derive(Debug)]
pub struct BobGetResult {
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub struct BobPingResult {
    pub node: Node,
}

pub struct BobData {
    pub data: Vec<u8>,
}

#[derive(Debug, Copy, Clone)]
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

#[derive(Debug, Clone)]
pub struct VDisk {
    pub id: u32,
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

#[derive(Clone, Eq)]
pub struct Node {
    pub host: String,
    pub port: u16,
}

impl Node {
    pub fn get_uri(&self) -> http::Uri {
        format!("http://{}", self).parse().unwrap()
    }
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
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
        write!(f, "{}:{}", self.host, self.port)
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
}

impl std::fmt::Display for NodeDisk {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}/{}", self.node, self.path)
    }
}

impl PartialEq for NodeDisk {
    fn eq(&self, other: &NodeDisk) -> bool {
        self.node == other.node && self.path == other.path
    }
}

pub fn print_vec<T: std::fmt::Display>(coll: &[T]) -> String {
    coll.iter()
        .map(|vd| vd.to_string())
        .collect::<Vec<_>>()
        .join(",")
}
