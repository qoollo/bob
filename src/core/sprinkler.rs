use crate::core::data::{BobKey, BobData, Node, NodeDisk, VDisk, BobError};
use crate::core::link_manager::LinkManager;

use tokio::prelude::*;
use std::time::{ Duration};
use std::sync::Arc;

use futures::future::*;
use futures::stream::*;





#[derive(Clone)]
pub struct Cluster {
    pub vdisks: Vec<VDisk>

}

#[derive(Clone)]
pub struct Sprinkler {
    cluster: Cluster,
    quorum: u8,
    link_manager: Arc<LinkManager>
}

pub struct SprinklerError {
    total_ops: u16,
    ok_ops: u16,
    quorum: u8

}

pub struct SprinklerResult {
    total_ops: u16,
    ok_ops: u16,
    quorum: u8
}

impl Sprinkler {
    pub fn new() -> Sprinkler {
        let ex_cluster = Cluster {
                vdisks: vec![VDisk {
                    id: 0,
                    replicas: vec![NodeDisk {
                        node: Node {
                            host: "127.0.0.1".to_string(),
                            port: 20000
                        },
                        path: "/tmp/disk1".to_string()
                    },
                    NodeDisk {
                        node: Node {
                            host: "127.0.0.1".to_string(),
                            port: 20002
                        },
                        path: "/tmp/disk2".to_string()
                    }]
                }]
            };
        let nodes: Vec<_> = ex_cluster.vdisks.iter()
                                    .flat_map(|vdisk| vdisk.replicas.iter().map(|nd| nd.node.clone())  )
                                    .collect();
        Sprinkler {
            quorum: 1,
            cluster: ex_cluster,
            link_manager: Arc::new(LinkManager::new(nodes, Duration::from_millis(3000)))
        }
    }

    pub fn get_periodic_tasks(&self, ex: tokio::runtime::TaskExecutor) -> Box<impl Future<Item=(), Error=()>> {
        self.link_manager.get_checker_future(ex)
    }

    pub fn put_clustered(&self, key: BobKey, data: BobData) 
            -> impl Future<Item = SprinklerResult, Error = SprinklerError> + 'static + Send {
        println!("PUT[{:?}]: Data size: {:?}", key, data.data.len());
        
        let target_vdisks: Vec<VDisk> = self.cluster.vdisks.iter()
            .filter(|disk| disk.id == 0)
            .cloned()
            .collect();

        
        println!("PUT[{:?}]: Will use this vdisks {:?}", key, target_vdisks);

        let mut target_nodes: Vec<Node> = target_vdisks.iter()
            .flat_map(|node_disk| node_disk.replicas.iter()
                                                    .map(|nd| nd.node.clone())
                                                    .collect::<Vec<Node>>())
            .collect();
        target_nodes.dedup();

        println!("PUT[{:?}]: Nodes for fan out: {:?}", key, target_nodes);

        // incupsulate vdisk in transaction
        let mut conn_to_send: Vec<_> = target_nodes.iter()
                                        .map(|n| self.link_manager.clone().get_link(n))
                                        .collect();

        let reqs: Vec<_> = conn_to_send.iter_mut().map(move |nl| {
            match &mut nl.conn {
                Some(conn) => Either::A(conn.put(key, &data)),
                None => Either::B(err(BobError::Other(format!("No active connection {:?}", nl.node))))
            }            
        }).collect();
        let l_quorum = self.quorum;
        Box::new(futures_unordered(reqs)
        .then(|r| {println!("req {:?}", r); Result::<_, ()>::Ok(r)})
        .fold(vec![], |mut acc, r| ok::<_,()>({acc.push(r); acc}))
        .then(move |acc| {
            let res = acc.unwrap();
            println!("PUT[{:?}] cluster ans: {:?}", key, res);
            let total_ops = res.iter().count();
            let ok_count = res.iter().filter(|&r| r.is_ok()).count();
            // TODO: send actuall list of vdisk it has been written on
            if ok_count >= l_quorum as usize {
                ok(SprinklerResult{
                    total_ops: total_ops as u16,
                    ok_ops: ok_count as u16,
                    quorum: l_quorum
                })
            } else {
                err(SprinklerError{
                    total_ops: total_ops as u16,
                    ok_ops: ok_count as u16,
                    quorum: l_quorum
                })
            }
        }))
    }
}