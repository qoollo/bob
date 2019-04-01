
#[derive(Debug)]
pub enum BobError {
    Timeout,
    
    Other(String)
}
#[derive(Debug)]
pub struct BobPutResult {

}

#[derive(Debug)]
pub struct BobPingResult {

}

pub struct BobData {
    pub data: Vec<u8>
}

#[derive(Debug, Copy, Clone)]
pub struct BobKey {
    pub key: u64
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
    pub replicas: Vec<NodeDisk>
}

#[derive(Clone, Eq)]
pub struct Node {
    pub host: String,
    pub port: u16,
}

impl Node {
    pub fn get_uri(&self) -> http::Uri {
        format!("http://{}:{}", self.host, self.port).parse().unwrap()
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
    pub path: String
}

impl PartialEq for NodeDisk {
    fn eq(&self, other: &NodeDisk) -> bool {
        self.node == other.node && self.path == other.path
    }
}