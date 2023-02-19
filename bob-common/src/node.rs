use crate::{
    bob_client::{BobClient, Factory},
    data::BobData,
};
use http::Uri;
use std::{
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
};

pub type NodeId = u16;

/// Node name struct. Clone is lightweight
#[derive(Clone)]
pub struct NodeName(Arc<str>);

#[derive(Clone)]
pub struct Node {
    // Arc<NodeInner> created to make `clone` lightweight
    inner: Arc<NodeInner>
}

/// Inner node data. 
/// Node is cloned many times, so to reduce the number of allocations, the internals are placed inside this structure and wrapped with Arc
struct NodeInner {
    index: NodeId,
    name: NodeName,
    address: String,

    // TODO: use ArcSwap
    conn: RwLock<Option<Arc<BobClient>>>,
    conn_available: AtomicBool,
}

#[derive(Debug)]
pub struct Output<T> {
    node_name: NodeName,
    inner: T,
}

#[derive(Debug, Clone)]
pub struct Disk {
    node_name: NodeName,
    disk_path: String,
    disk_name: String,
}



impl Node {
    pub fn new(name: &str, address: &str, index: u16) -> Self {
        Self {
            inner: NodeInner {
                index,
                name: name.into(),
                address: address.to_string(),
                conn: RwLock::new(None),
                conn_available: AtomicBool::new(false),
            }
        }
    }

    pub fn name(&self) -> &NodeName {
        &self.inner.name
    }

    pub fn index(&self) -> NodeId {
        self.inner.index
    }

    pub fn address(&self) -> &str {
        &self.inner.address
    }

    pub fn get_uri(&self) -> Uri {
        Uri::builder()
            .scheme("http")
            .authority(self.address())
            .path_and_query("/")
            .build()
            .expect("build uri")
    }

    pub fn counter_display(&self) -> String {
        self.address().to_string().replace('.', "_")
    }

    pub fn set_connection(&self, client: BobClient) {
        let mut conn = self.inner.conn.write().expect("rwlock write error");
        *conn = Some(Arc::new(client));
        self.inner.conn_available.store(true, Ordering::Release);
    }

    pub fn clear_connection(&self) {
        let mut conn = self.inner.conn.write().expect("rwlock write error");
        *conn = None;
        self.inner.conn_available.store(false, Ordering::Release);
    }

    pub fn get_connection(&self) -> Option<Arc<BobClient>> {
        self.inner.conn.read().expect("rwlock").clone()
    }

    pub fn connection_available(&self) -> bool {
        self.inner.conn_available.load(Ordering::Acquire)
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
        self.inner.address.hash(state);
    }
}

impl Debug for Node {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}={}", self.inner.name, self.inner.address)
    }
}

// @TODO get off partialeq trait
impl PartialEq for Node {
    fn eq(&self, other: &Node) -> bool {
        self.inner.address == other.inner.address
    }
}

impl Eq for Node {}

impl<T> Output<T> {
    pub fn new(node_name: NodeName, inner: T) -> Self {
        Self { node_name, inner }
    }

    pub fn node_name(&self) -> &NodeName {
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
    pub fn new(disk_path: String, disk_name: String, node_name: NodeName) -> Self {
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

    pub fn node_name(&self) -> &NodeName {
        &self.node_name
    }
}

impl PartialEq for Disk {
    fn eq(&self, other: &Disk) -> bool {
        self.node_name == other.node_name && self.disk_name == other.disk_name
    }
}



// ============= NodeName =============

impl From<&str> for NodeName {
    fn from(val: &str) -> Self {
        Self(val.into())
    }
}

impl From<&String> for NodeName {
    fn from(val: &String) -> Self {
        Self(val.into())
    }
}

impl From<String> for NodeName {
    fn from(val: String) -> Self {
        Self(val.into())
    }
}

impl AsRef<str> for NodeName {
    fn as_ref(&self) -> &str {
        self.as_ref()
    }
}

impl PartialEq for NodeName {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ref() == other.0.as_ref()
    }
}

impl Eq for NodeName { }

impl PartialEq<str> for NodeName {
    fn eq(&self, other: &str) -> bool {
        self.0.as_ref() == other
    }
}

impl PartialEq<NodeName> for str {
    fn eq(&self, other: &NodeName) -> bool {
        self == other.0.as_ref()
    }
}

impl PartialEq<String> for NodeName {
    fn eq(&self, other: &String) -> bool {
        self.0.as_ref() == other
    }
}

impl PartialEq<NodeName> for String {
    fn eq(&self, other: &NodeName) -> bool {
        self == other.0.as_ref()
    }
}

impl Hash for NodeName {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.as_ref().hash(state)
    }
}

impl Debug for NodeName {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("NodeName").field(&self.0.as_ref()).finish()
    }
}

impl Display for NodeName {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str(self.0.as_ref())
    }
}