extern crate itertools;

use itertools::Itertools;
use std::fs;


pub trait Validatable {
    fn validate(&self) -> bool;
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeDisk {
    pub path: String,
    pub name: String,
}
 
impl Validatable for NodeDisk {
     fn validate(&self) -> bool {
        // TODO log
        !self.path.is_empty() && !self.name.is_empty()
            && self.path != "~" && self.name != "~"
    }
 }

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub name: String,
    pub address: String,
    pub disks: Vec<NodeDisk>,
}

impl Validatable for Node {
    fn validate(&self) -> bool {
        //TODO log

        !self.address.is_empty() && !self.name.is_empty()
            && self.address != "~" && self.name != "~"
            && self.disks.iter().all(|x| x.validate())
            && self.disks.iter()
            .group_by(|x| x.name.clone())
            .into_iter()
            .map(|(_, group)| group.count())
            .filter(|x| *x > 1)
            .count() == 0
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Replica {
    pub node: String,
    pub disk: String,
}

impl PartialEq for Replica {
    fn eq(&self, other: &Replica) -> bool {
        self.node == other.node && self.disk == other.disk
    }
}

impl Validatable for Replica {
    fn validate(&self) -> bool {
        // TODO log
        !self.node.is_empty() && !self.disk.is_empty()
            && self.node != "~" && self.disk != "~"
    }
 }

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct VDisk {
    pub id: i32,
    pub replicas: Vec<Replica>,
}

impl Validatable for VDisk {
    fn validate(&self) -> bool {
        //TODO log
        true && self.replicas.iter().all(|x| x.validate())
            && self.replicas.iter()
            .group_by(|x| x.clone())
            .into_iter()
            .map(|(_, group)| group.count())
            .filter(|x| *x > 1)
            .count() == 0
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Cluster {
    pub nodes: Vec<Node>,
    pub vdisks: Vec<VDisk>
}

impl Validatable for Cluster {
    fn validate(&self) -> bool {
        if self.nodes.len() == 0
            || self.vdisks.len() == 0
            || self.nodes.iter().any(|x| !x.validate()) 
            || self.vdisks.iter().any(|x| !x.validate()) {
            return false;
        }

        if self.vdisks.iter()
            .group_by(|x| x.id)
            .into_iter()
            .map(|(_, group)| group.count())
            .filter(|x| *x > 1)
            .count() != 0{
            return false;
        }

        if self.nodes.iter()
            .group_by(|x| x.name.clone())
            .into_iter()
            .map(|(_, group)| group.count())
            .filter(|x| *x > 1)
            .count() != 0{
            return false;
        }

        for vdisk in self.vdisks.iter() {
            for replica in vdisk.replicas.iter() {
                match self.nodes.iter().find(|x|x.name==replica.node) {
                    Some(node) => {
                        if node.disks.iter().find(|x|x.name==replica.disk) == None {
                            //TODO log cannot find disk in node
                            return false;
                        }
                    },
                    None    => {
                        //TODO log cannot find node
                        return false;
                    },
                }
            }
        }

        true
    }
}

pub fn read_config(filename: &String) -> Option<Cluster> {
    let result:Result<String,_> = fs::read_to_string(filename);
    match result {
        Ok(config) => return parse_config(&config),
        Err(e) => {
            //TODO log
            return None;
        }
    }
}

pub fn parse_config(config: &String) -> Option<Cluster> {
    let result:Result<Cluster, _> = serde_yaml::from_str(config);
    match result {
        Ok(cluster) => return Some(cluster),
        Err(e) => {
            //TODO log
            return None;
        }
    }
}