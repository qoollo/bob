use super::name_types::{Name, NameMarker};
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

/// Marker type for [`NodeName`]
pub struct OfNode;
impl NameMarker for OfNode {
    fn display_name() -> &'static str {
        "NodeName"
    }
}

pub type NodeId = u16;
/// Node name. Clone is lightweight
pub type NodeName = Name<OfNode>;

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

    conn: RwLock<Option<Arc<BobClient>>>,
    conn_available: AtomicBool,
}

#[derive(Debug)]
pub struct Output<T> {
    node_name: NodeName,
    inner: T,
}


impl Node {
    pub fn new(name: NodeName, address: String, index: u16) -> Self {
        Self {
            inner: Arc::new(NodeInner {
                index,
                name,
                address,
                conn: RwLock::new(None),
                conn_available: AtomicBool::new(false),
            })
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


impl PartialEq for Node {
    fn eq(&self, other: &Node) -> bool {
        self.inner.name == other.inner.name
    }
}

impl Eq for Node {}

impl Hash for Node {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.name.hash(state);
    }
}

impl Debug for Node {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}={}", self.inner.name, self.inner.address)
    }
}


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

    pub fn with_node_name(self, node_name: NodeName) -> Output<T> {
        Self {
            node_name,
            inner: self.inner
        }
    }
}

impl Output<BobData> {
    pub fn timestamp(&self) -> u64 {
        self.inner.meta().timestamp()
    }
}
