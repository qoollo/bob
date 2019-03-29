use crate::core::data::Node;
use crate::core::bob_client::BobClient;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::timer::Interval;
use futures::stream::Stream;
use futures::future::Future;
use std::time::{ Duration};

pub struct NodeLink {
    pub node: Node,
    pub conn: Option<BobClient>
}

#[derive(Clone)]
pub struct NodeLinkHolder {
    node: Node,
    conn: Arc<Mutex<Option<BobClient>>>
}

impl NodeLinkHolder {
    pub fn new(node: Node) -> NodeLinkHolder {
        NodeLinkHolder {
            node,
            conn: Arc::new(Mutex::new(None))
        }
    }

    pub fn get_connection(&self) -> NodeLink {
        NodeLink {
            node: self.node.clone(),
            conn: self.conn.lock().unwrap().clone()
        }
    }

    pub fn set_connection(&self, client: BobClient) {
        *self.conn.lock().unwrap() = Some(client);
    }

    pub fn clear_connection(&self) {
        *self.conn.lock().unwrap() = None;
    }
}

pub struct LinkManager {
    repo: Arc<HashMap<Node, NodeLinkHolder>>,
    check_interval: Duration,
    timeout: Duration
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
            timeout
        }
    }

    pub fn get_checker_future(&self, ex: tokio::runtime::TaskExecutor) -> Box<impl Future<Item=(), Error=()>> {
        let local_repo = self.repo.clone();
        let timeout = self.timeout;
        Box::new(
            Interval::new_interval(self.check_interval)
            .for_each(move |_| {
                for (_, v) in local_repo.iter() {
                        match v.get_connection().conn {
                        Some(mut conn) => {
                            let lv = v.clone();
                            tokio::spawn(
                                conn.ping()
                                    .then(move |r| { 
                                            match r {
                                                Ok(_) => println!("All good with pinging node {:?}", lv.node),
                                                Err(_) => {
                                                    println!("Got broken connection to node {:?}", lv.node);
                                                    lv.clear_connection();
                                                }
                                            };
                                            Ok(())
                                        })
                            );
                        },
                        None => {
                            let lv = v.clone();
                            println!("will esteblish new connection to {:?}", v.node);
                            tokio::spawn(BobClient::new(lv.node.clone(), ex.clone(), timeout).map(move |client| {
                                 lv.set_connection(client);
                             })
                            );  
                        }
                    };
                };

                Ok(())
            })
            .map_err(|e| panic!("can't make to work timer {:?}", e))
            
        )
    }

    pub fn get_link(&self, node: &Node) -> NodeLink {
        match self.repo.get(node) {
            Some(link) => link.get_connection(),
            None => panic!("No such node in repo. Check config and cluster setup")
        }
    }
}