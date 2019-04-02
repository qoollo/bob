use crate::core::bob_client::{BobClient, BobClientFactory};
use crate::core::data::Node;
use futures::future::Either;
use futures::future::Future;
use futures::stream::Stream;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::timer::Interval;

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
            conn: Arc::new(Mutex::new(None)),
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

    fn check(&self, client_fatory: BobClientFactory) -> impl Future<Item = (), Error = ()> {
        match self.get_connection().conn {
            Some(mut conn) => {
                let nlh = self.clone();
                Either::A(conn.ping().then(move |r| {
                    match r {
                        Ok(_) => debug!("All good with pinging node {:?}", nlh.node),
                        Err(_) => {
                            debug!("Got broken connection to node {:?}", nlh.node);
                            nlh.clear_connection();
                        }
                    };
                    Ok(())
                }))
            }
            None => {
                let nlh = self.clone();
                debug!("will connect to {:?}", nlh.node);
                Either::B(client_fatory.produce(nlh.node.clone()).map(move |client| {
                    nlh.set_connection(client);
                }))
            }
        }
    }
}

pub struct LinkManager {
    repo: Arc<HashMap<Node, NodeLinkHolder>>,
    check_interval: Duration,
    timeout: Duration,
}

impl LinkManager {
    pub fn new(nodes: Vec<Node>, timeout: Duration) -> LinkManager {
        LinkManager {
            repo: {
                let mut hm = HashMap::new();
                for node in nodes {
                    hm.insert(node.clone(), NodeLinkHolder::new(node));
                }
                Arc::new(hm)
            },
            check_interval: Duration::from_millis(5000),
            timeout,
        }
    }

    pub fn get_checker_future(
        &self,
        ex: tokio::runtime::TaskExecutor,
    ) -> Box<impl Future<Item = (), Error = ()>> {
        let local_repo = self.repo.clone();
        let client_factory = BobClientFactory {
            executor: ex,
            timeout: self.timeout,
        };
        Box::new(
            Interval::new_interval(self.check_interval)
                .for_each(move |_| {
                    local_repo.values().for_each(|v| {
                        tokio::spawn(v.check(client_factory.clone()));
                    });

                    Ok(())
                })
                .map_err(|e| panic!("can't make to work timer {:?}", e)),
        )
    }

    pub fn get_link(&self, node: &Node) -> NodeLink {
        self.repo
            .get(node)
            .expect("No such node in repo. Check config and cluster setup")
            .get_connection()
    }
}
