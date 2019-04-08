use std::panic;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeDisk {
    pub path: String,
    pub name: String,
}
 
 impl NodeDisk {
    pub fn validate(&self) -> bool {
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

impl Node {
    pub fn validate(&self) -> bool {
        //TODO log
        !self.address.is_empty() && !self.name.is_empty()
            && self.address != "~" && self.name != "~"
            && self.disks.iter().all(|x| x.validate())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Replica {
    pub node: String,
    pub disk: String,
}

impl Replica {
    pub fn validate(&self) -> bool {
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

impl VDisk {
    pub fn validate(&self) -> bool {
        //TODO log
        true && self.replicas.iter().all(|x| x.validate())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Cluster {
    pub nodes: Vec<Node>,
    pub vdisks: Vec<VDisk>
}

impl Cluster {
    pub fn validate(&self) -> bool {
        if self.nodes.len() == 0
            || self.vdisks.len() == 0
            || self.nodes.iter().any(|x| !x.validate()) 
            || self.vdisks.iter().any(|x| !x.validate()) {
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

use std::fs;

pub fn read_config(filename: &String) -> Option<Cluster> {
    let result:Result<String,_> = fs::read_to_string(filename);
    match result {
        Ok(config) => return parse_config(&config),
        Err(e) => {
            //TODO log
            return None
        }
    }
}

pub fn parse_config(config: &String) -> Option<Cluster> {
    let result:Result<Cluster, _> = serde_yaml::from_str(config);
    match result {
        Ok(cluster) => return Some(cluster),
        Err(e) => {
            //TODO log
            return None
        }
    }
}