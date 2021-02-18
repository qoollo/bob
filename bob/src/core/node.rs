use std::hash::{Hash, Hasher};

use super::prelude::*;

pub type ID = u16;

pub type Name = String;

#[derive(Clone)]
pub(crate) struct Node {
    name: Name,
    address: SocketAddr,
    index: ID,
    conn: Arc<RwLock<Option<BobClient>>>,
}

#[derive(Debug)]
pub(crate) struct Output<T> {
    node_name: Name,
    inner: T,
}

#[derive(Debug, Clone)]
pub(crate) struct Disk {
    node_name: Name,
    disk_path: String,
    disk_name: String,
}

impl Node {
    pub(crate) async fn new(name: String, address: &str, index: u16) -> Self {
        error!("address: [{}]", address);
        let mut address = lookup_host(address).await.expect("DNS resolution failed");
        let address = address.next().expect("address is empty");
        Self {
            name,
            address,
            index,
            conn: Arc::default(),
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn index(&self) -> ID {
        self.index
    }

    pub(crate) fn address(&self) -> &SocketAddr {
        &self.address
    }

    pub(crate) fn get_uri(&self) -> Uri {
        Uri::builder()
            .scheme("http")
            .authority(self.address.to_string().as_str())
            .path_and_query("/")
            .build()
            .expect("build uri")
    }

    pub(crate) fn counter_display(&self) -> String {
        self.address.to_string().replace(".", "_")
    }

    pub(crate) async fn set_connection(&self, client: BobClient) {
        *self.conn.write().await = Some(client);
    }

    pub(crate) async fn clear_connection(&self) {
        *self.conn.write().await = None;
    }

    pub(crate) async fn get_connection(&self) -> Option<BobClient> {
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
        self.address.hash(state);
    }
}

impl Debug for Node {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}={}", self.name, self.address)
    }
}

// @TODO get off partialeq trait
impl PartialEq for Node {
    fn eq(&self, other: &Node) -> bool {
        self.address == other.address
    }
}

impl Eq for Node {}

impl<T> Output<T> {
    pub(crate) fn new(node_name: Name, inner: T) -> Self {
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

impl Output<BobData> {
    pub(crate) fn timestamp(&self) -> u64 {
        self.inner.meta().timestamp()
    }
}

impl Disk {
    pub(crate) fn new(disk_path: String, disk_name: String, node_name: Name) -> Self {
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

impl PartialEq for Disk {
    fn eq(&self, other: &Disk) -> bool {
        self.node_name == other.node_name && self.disk_name == other.disk_name
    }
}
