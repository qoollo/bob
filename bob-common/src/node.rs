use crate::{
    bob_client::{BobClient, Factory},
    data::BobData,
};
use http::Uri;
use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock
    },
};

pub type Id = u16;

pub type Name = String;

#[derive(Clone)]
pub struct Node {
    name: Name,
    address: String,
    index: Id,
    conn: Arc<RwLock<Option<BobClient>>>,
    err_count: Arc<AtomicUsize>,
    max_sequential_errors: usize,
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
    pub async fn new(
        name: String,
        address: &str,
        index: u16,
        max_sequential_errors: usize,
    ) -> Self {
        Self {
            name,
            address: address.to_string(),
            index,
            conn: Arc::default(),
            err_count: Arc::default(),
            max_sequential_errors,
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

    pub fn set_connection(&self, client: BobClient) {
        *self.conn.write().expect("rwlock") = Some(client);
        self.reset_error_count();
    }

    pub fn counter_display(&self) -> String {
        self.address.to_string().replace('.', "_")
    }

    pub fn clear_connection(&self) {
        *self.conn.write().expect("rwlock") = None;
    }

    pub fn get_connection(&self) -> Option<BobClient> {
        self.conn.read().expect("rwlock").clone()
    }

    pub fn reset_error_count(&self) {
        self.err_count.store(0, Ordering::Relaxed);
    }

    pub async fn increase_error_and_clear_conn_if_needed(&self) {
        let count = self.err_count.fetch_add(1, Ordering::Relaxed);
        if count >= self.max_sequential_errors {
            self.clear_connection();
        }
    }

    pub async fn check(&self, client_factory: &Factory) -> Result<(), String> {
        if let Some(conn) = self.get_connection() {
            self.ping(&conn).await
        } else {
            debug!("will connect to {:?}", self);
            let client = client_factory.produce(self.clone()).await?;
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
            inner: map_fn(self.inner)
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
