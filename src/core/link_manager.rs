use crate::core::{
    backend::Error,
    bob_client::{BobClient, BobClientFactory},
    data::{ClusterResult, Node},
};
use futures::{future::Future, stream::Stream};
use std::{
    collections::HashMap,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures03::{
    compat::Future01CompatExt,
    future::{err, FutureExt as OtherFutureExt, TryFutureExt},
    task::{Spawn, SpawnExt},
    Future as Future03,
};

use tokio_timer::Interval;

pub struct NodeLink {
    pub node: Node,
    pub conn: Option<BobClient>,
}

#[derive(Clone)]
pub struct NodeLinkHolder {
    node: Node,
    conn: Arc<Mutex<Option<BobClient>>>,
}

impl NodeLinkHolder {
    pub fn new(node: Node) -> NodeLinkHolder {
        NodeLinkHolder {
            node,
            conn: Arc::new(Mutex::new(None)), // TODO: consider to use RwLock
        }
    }

    pub fn get_connection(&self) -> NodeLink {
        NodeLink {
            node: self.node.clone(),
            conn: self.conn.lock().unwrap().clone(),
        }
    }

    pub fn set_connection(&self, client: BobClient) {
        *self.conn.lock().unwrap() = Some(client);
    }

    pub fn clear_connection(&self) {
        *self.conn.lock().unwrap() = None;
    }

    async fn check(self, client_fatory: BobClientFactory) -> Result<(), ()> {
        match self.get_connection().conn {
            Some(mut conn) => {
                let nlh = self.clone();
                conn.ping()
                    .await
                    .map(|_| debug!("All good with pinging node {:?}", nlh.node))
                    .map_err(|_| {
                        debug!("Got broken connection to node {:?}", nlh.node);
                        nlh.clear_connection();
                    })?;
                Ok(())
            }
            None => {
                let nlh = self.clone();
                debug!("will connect to {:?}", nlh.node);
                client_fatory
                    .produce(nlh.node.clone())
                    .await
                    .map(move |client| {
                        nlh.set_connection(client);
                    })
            }
        }
    }
}

pub struct LinkManager {
    repo: Arc<HashMap<Node, NodeLinkHolder>>,
    check_interval: Duration,
}

pub type ClusterCallType<T> = Result<ClusterResult<T>, ClusterResult<Error>>;
pub type ClusterCallFuture<T> =
    Pin<Box<dyn Future03<Output = ClusterCallType<T>> + 'static + Send>>;

impl LinkManager {
    pub fn new(nodes: Vec<Node>, check_interval: Duration) -> LinkManager {
        LinkManager {
            repo: {
                let mut hm = HashMap::new();
                for node in nodes {
                    hm.insert(node.clone(), NodeLinkHolder::new(node));
                }
                Arc::new(hm)
            },
            check_interval,
        }
    }

    pub async fn get_checker_future<S>(
        &self,
        client_factory: BobClientFactory,
        spawner: S,
    ) -> Result<(), ()>
    where
        S: Spawn + Clone + Send + 'static + Unpin + Sync,
    {
        let local_repo = self.repo.clone();
        Interval::new_interval(self.check_interval)
            .for_each(move |_| {
                local_repo.values().for_each(|v| {
                    let q = v.clone().check(client_factory.clone());
                    let _ = spawner
                        .clone()
                        .spawn(q.map(|_r| {}))
                        .map_err(|e| panic!("can't run timer task {:?}", e));
                });

                Ok(())
            })
            .map_err(|e| panic!("can't make to work timer {:?}", e))
            .compat()
            .boxed()
            .await
    }

    pub fn get_link(&self, node: &Node) -> NodeLinkHolder {
        self.repo
            .get(node)
            .expect("No such node in repo. Check config and cluster setup")
            .clone()
    }

    pub fn get_connections(&self, nodes: &[Node]) -> Vec<NodeLinkHolder> {
        nodes.iter().map(|n| self.get_link(n)).collect()
    }

    pub fn call_nodes<F: Send, T: 'static + Send>(
        &self,
        nodes: &[Node],
        mut f: F,
    ) -> Vec<ClusterCallFuture<T>>
    where
        F: FnMut(&mut BobClient) -> ClusterCallFuture<T>,
    {
        let links = &mut self.get_connections(nodes);
        let t: Vec<_> = links
            .iter_mut()
            .map(move |nl| {
                let node = nl.node.clone();
                let nl_clone = nl.clone();
                match &mut nl.get_connection().conn {
                    Some(conn) => f(conn)
                        .boxed()
                        .map_err(move |e| {
                            if e.result.is_service() {
                                nl_clone.clear_connection();
                            }
                            e
                        })
                        .boxed(),
                    None => err(ClusterResult {
                        result: Error::Failed(format!("No active connection {:?}", node)),
                        node,
                    })
                    .boxed(),
                }
            })
            .collect();
        t
    }
}
