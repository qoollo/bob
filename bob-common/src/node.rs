use crate::{
    bob_client::{BobClient, Factory},
    data::BobData,
};
use http::Uri;
use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
};

pub type Id = u16;

pub type Name = String;

#[derive(Clone)]
pub struct Node {
    name: Name,
    address: String,
    index: Id,
    conn: Arc<RwLock<Option<Arc<BobClient>>>>,
    conn_available: Arc<AtomicBool>,
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
    pub fn new(name: String, address: &str, index: u16) -> Self {
        Self {
            name,
            address: address.to_string(),
            index,
            conn: Arc::default(),
            conn_available: Arc::default(),
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
        self.address.to_string().replace('.', "_")
    }

    pub fn set_connection(&self, client: BobClient) {
        let mut conn = self.conn.write().expect("rwlock write error");
        *conn = Some(Arc::new(client));
        self.conn_available.store(true, Ordering::Release);
    }

    pub fn clear_connection(&self) {
        let mut conn = self.conn.write().expect("rwlock write error");
        *conn = None;
        self.conn_available.store(false, Ordering::Release);
    }

    pub fn get_connection(&self) -> Option<Arc<BobClient>> {
        self.conn.read().expect("rwlock").clone()
    }

    pub fn connection_available(&self) -> bool {
        self.conn_available.load(Ordering::Acquire)
    }

    pub async fn check(&self, client_factory: &Factory) -> Result<(), String> {
        if let Some(conn) = self.get_connection() {
            self.ping(conn.as_ref()).await
        } else {
            debug!("will connect to {:?}", self);
            let client = client_factory.produce(&self).await?;
            self.ping(&client).await?;
            self.set_connection(client);
            Ok(())
        }
    }

    pub async fn ping(&self, conn: &BobClient) -> Result<(), String> {
        if let Err(e) = conn.ping().await {
            debug!("Got broken connection to node {:?}", self);
            self.clear_connection();
            Err(format!("{:?}", e))
        } else {
            debug!("All good with pinging node {:?}", self);
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

    pub fn map<TOut>(self, map_fn: impl FnOnce(T) -> TOut) -> Output<TOut> {
        Output::<TOut> {
            node_name: self.node_name,
            inner: map_fn(self.inner),
        }
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
