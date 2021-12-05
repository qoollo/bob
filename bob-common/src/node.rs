use crate::{
    bob_client::{BobClient, Factory},
    data::BobData,
};
use http::Uri;
use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    hash::{Hash, Hasher},
    sync::Arc,
};
use tokio::sync::RwLock;

pub type Id = u16;

pub type Name = String;

#[derive(Clone)]
pub struct Node {
    name: Name,
    address: String,
    index: Id,
    conn: Arc<RwLock<Option<BobClient>>>,
}

#[derive(Debug)]
pub struct Output<T> {
    node_name: Name,
    inner: T,
}

#[derive(Debug, Clone)]
pub struct Disk {
    node_name: Name,
    disk_path: String,
    disk_name: String,
}

impl Node {
    pub async fn new(name: String, address: &str, index: u16) -> Self {
        Self {
            name,
            address: address.to_string(),
            index,
            conn: Arc::default(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn index(&self) -> Id {
        self.index
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn get_uri(&self) -> Uri {
        Uri::builder()
            .scheme("http")
            .authority(self.address.to_string().as_str())
            .path_and_query("/")
            .build()
            .expect("build uri")
    }

    pub fn counter_display(&self) -> String {
        self.address.to_string().replace(".", "_")
    }

    pub async fn set_connection(&self, client: BobClient) {
        *self.conn.write().await = Some(client);
    }

    pub async fn clear_connection(&self) {
        *self.conn.write().await = None;
    }

    pub async fn get_connection(&self) -> Option<BobClient> {
        self.conn.read().await.clone()
    }

    pub async fn check(&self, client_fatory: &Factory) -> Result<(), String> {
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
    pub fn new(node_name: Name, inner: T) -> Self {
        Self { node_name, inner }
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn inner(&self) -> &T {
        &self.inner
    }

    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl Output<BobData> {
    pub fn timestamp(&self) -> u64 {
        self.inner.meta().timestamp()
    }
}

impl Disk {
    pub fn new(disk_path: String, disk_name: String, node_name: Name) -> Self {
        Self {
            node_name,
            disk_path,
            disk_name,
        }
    }

    pub fn disk_path(&self) -> &str {
        &self.disk_path
    }

    pub fn disk_name(&self) -> &str {
        &self.disk_name
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }
}

impl PartialEq for Disk {
    fn eq(&self, other: &Disk) -> bool {
        self.node_name == other.node_name && self.disk_name == other.disk_name
    }
}
