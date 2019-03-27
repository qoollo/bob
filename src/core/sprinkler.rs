use crate::core::data::{BobKey, BobData, Node, NodeDisk, VDisk};
use crate::core::net_abs::BobClient;
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
    put_timeout: Duration,
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
            put_timeout: Duration::from_millis(10000),
            quorum: 1,
            cluster: ex_cluster,
            link_manager: Arc::new(LinkManager::new(nodes))
        }
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
        let conn_to_send: Vec<_> = target_nodes.iter()
                                        .map(|n| self.link_manager.clone()
                                        .get_link(n))
                                        .filter_map(|l| l)
                                        .collect();
        // let conn_to_send:Vec<BobClient> = self.cluster.nodes.iter()
        //                                                 .filter(|nc| target_nodes.contains(&nc.node) )
        //                                                 .map(|nc| nc.conn.clone())
        //                                                 .collect();
        let reqs: Vec<_> = conn_to_send.iter().map(|c| {
            //Timeout::new(c.put(&key, &data).join_metadata_result(), self.put_timeout)
            // TODO: some issue with temout. Will think about it later
            c.put(&key, &data)
        }).collect();
        let l_quorum = self.quorum;
        Box::new(futures_unordered(reqs)
        .then(|r| {println!("req {:?}", r); Result::<_, ()>::Ok(r)})
        .fold(vec![], |mut acc, r| ok::<_,()>({acc.push(r); acc}))
        .then(move |acc| {
            let res = acc.unwrap();
            println!("PUT[{:?}] cluster ans: {:?}", key, res);
            let total_ops = res.iter().count();
            let ok_count = res.iter().filter(|&r| match r {
                Ok(_) => true,
                Err(_) => false
            }).count();
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