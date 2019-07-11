use itertools::Itertools;
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    net::SocketAddr,
};

use crate::core::{
    configs::reader::{Validatable, YamlBobConfigReader},
    data::{Node as DataNode, NodeDisk as DataNodeDisk, VDisk as DataVDisk, VDiskId},
};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeDisk {
    pub path: Option<String>,
    pub name: Option<String>,
}

impl NodeDisk {
    pub fn name(&self) -> String {
        self.name.as_ref().unwrap().clone()
    }
    pub fn path(&self) -> String {
        self.path.as_ref().unwrap().clone()
    }
}
impl Validatable for NodeDisk {
    fn validate(&self) -> Result<(), String> {
        match &self.name {
            None => {
                debug!("field 'name' for 'disk' is not set");
                return Err("field 'name' for 'disk' is not set".to_string());
            }
            Some(name) => {
                if name.is_empty() {
                    debug!("field 'name' for 'disk' is empty");
                    return Err("field 'name' for 'disk' is empty".to_string());
                }
            }
        };
        match &self.path {
            None => {
                debug!("field 'path' for 'disk' is not set");
                return Err("field 'path' for 'disk' is not set".to_string());
            }
            Some(path) => {
                if path.is_empty() {
                    debug!("field 'path' for 'disk' is empty");
                    return Err("field 'path' for 'disk' is empty".to_string());
                }
            }
        };
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub name: Option<String>,
    pub address: Option<String>,
    #[serde(default)]
    pub disks: Vec<NodeDisk>,

    #[serde(skip)]
    pub host: RefCell<String>,
    #[serde(skip)]
    pub port: Cell<u16>,
}

impl Node {
    pub fn name(&self) -> String {
        self.name.as_ref().unwrap().clone()
    }
    pub fn address(&self) -> String {
        self.address.as_ref().unwrap().clone()
    }

    fn prepare(&self) -> Option<String> {
        let addr: Result<SocketAddr, _> = self.address().parse();
        if addr.is_err() {
            debug!(
                "field 'address': {} for 'Node': {} is invalid",
                self.address(),
                self.name()
            );
            return Some(format!(
                "field 'address': {} for 'Node': {} is invalid",
                self.address(),
                self.name()
            ));
        }
        let ip = addr.unwrap();
        self.host.replace(ip.ip().to_string());
        self.port.set(ip.port());
        None
    }
}
impl Validatable for Node {
    fn validate(&self) -> Result<(), String> {
        match &self.name {
            None => {
                debug!("field 'name' for 'Node' is not set");
                return Err("field 'name' for 'Node' is not set".to_string());
            }
            Some(name) => {
                if name.is_empty() {
                    debug!("field 'name' for 'Node' is empty");
                    return Err("field 'name' for 'Node' is empty".to_string());
                }
            }
        };
        match &self.address {
            None => {
                debug!("field 'address' for 'Node' is not set");
                return Err("field 'address' for 'Node' is not set".to_string());
            }
            Some(address) => {
                if address.is_empty() {
                    debug!("field 'address' for 'Node' is empty");
                    return Err("field 'address' for 'Node' is empty".to_string());
                }
            }
        };

        let result = self.aggregate(&self.disks);
        if result.is_err() {
            debug!("node is invalid: {} {}", self.name(), self.address());
            return result;
        }

        if self
            .disks
            .iter()
            .group_by(|x| x.name.clone())
            .into_iter()
            .map(|(_, group)| group.count())
            .filter(|x| *x > 1)
            .count()
            != 0
        {
            debug!("node: {} contains duplicate disk names", self.name());
            return Err(format!(
                "node: {} contains duplicate disk names",
                self.name()
            ));
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Replica {
    pub node: Option<String>,
    pub disk: Option<String>,
}

impl Replica {
    pub fn node(&self) -> String {
        self.node.as_ref().unwrap().clone()
    }
    pub fn disk(&self) -> String {
        self.disk.as_ref().unwrap().clone()
    }
}
impl Validatable for Replica {
    fn validate(&self) -> Result<(), String> {
        match &self.node {
            None => {
                debug!("field 'node' for 'Replica' is not set");
                return Err("field 'node' for 'Replica' is not set".to_string());
            }
            Some(node) => {
                if node.is_empty() {
                    debug!("field 'node' for 'Replica' is empty");
                    return Err("field 'node' for 'Replica' is empty".to_string());
                }
            }
        };
        match &self.disk {
            None => {
                debug!("field 'disk' for 'Replica' is not set");
                return Err("field 'disk' for 'Replica' is not set".to_string());
            }
            Some(disk) => {
                if disk.is_empty() {
                    debug!("field 'disk' for 'Replica' is empty");
                    return Err("field 'disk' for 'Replica' is empty".to_string());
                }
            }
        };
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct VDisk {
    pub id: Option<i32>,
    #[serde(default)]
    pub replicas: Vec<Replica>,
}

impl VDisk {
    pub fn id(&self) -> i32 {
        self.id.unwrap()
    }
}

impl Validatable for VDisk {
    fn validate(&self) -> Result<(), String> {
        if self.id.is_none() {
            debug!("field 'id' for 'VDisk' is not set");
            return Err("field 'id' for 'VDisk' is not set".to_string());
        }

        if self.replicas.is_empty() {
            debug!("vdisk must have replicas: {}", self.id());
            return Err(format!("vdisk must have replicas: {}", self.id()));
        }
        let result = self.aggregate(&self.replicas);
        if result.is_err() {
            debug!("vdisk is invalid: {}", self.id());
            return result;
        }

        if self
            .replicas
            .iter()
            .group_by(|&x| x.clone())
            .into_iter()
            .map(|(_, group)| group.count())
            .filter(|x| *x > 1)
            .count()
            != 0
        {
            debug!("vdisk: {} contains duplicate replicas", self.id());
            return Err(format!("vdisk: {} contains duplicate replicas", self.id()));
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ClusterConfig {
    #[serde(default)]
    pub nodes: Vec<Node>,
    #[serde(default)]
    pub vdisks: Vec<VDisk>,
}

impl Validatable for ClusterConfig {
    fn validate(&self) -> Result<(), String> {
        if self.nodes.is_empty() {
            debug!("no nodes in config");
            return Err("no nodes in config".to_string());
        }
        if self.vdisks.is_empty() {
            debug!("no vdisks in config");
            return Err("no vdisks in config".to_string());
        }
        let mut result = self.aggregate(&self.nodes);
        if result.is_err() {
            debug!("some nodes in config are invalid");
            return result;
        }
        result = self.aggregate(&self.vdisks);
        if result.is_err() {
            debug!("some vdisks in config are invalid");
            return result;
        }

        if self
            .vdisks
            .iter()
            .group_by(|x| x.id)
            .into_iter()
            .map(|(_, group)| group.count())
            .filter(|x| *x > 1)
            .count()
            != 0
        {
            debug!("config contains duplicates vdisks ids");
            return Err("config contains duplicates vdisks ids".to_string());
        }
        if self
            .nodes
            .iter()
            .group_by(|x| x.name.clone())
            .into_iter()
            .map(|(_, group)| group.count())
            .filter(|x| *x > 1)
            .count()
            != 0
        {
            debug!("config contains duplicates nodes names");
            return Err("config contains duplicates nodes names".to_string());
        }

        let err = self
            .nodes
            .iter()
            .filter_map(|x| x.prepare())
            .fold("".to_string(), |acc, x| acc + "\n" + &x);
        if !err.is_empty() {
            return Err(err);
        }

        for vdisk in self.vdisks.iter() {
            for replica in vdisk.replicas.iter() {
                match self.nodes.iter().find(|x| x.name == replica.node) {
                    Some(node) => {
                        if node.disks.iter().find(|x| x.name == replica.disk) == None {
                            debug!(
                                "cannot find in node: {:?}, disk with name: {:?} for vdisk: {:?}",
                                replica.node, replica.disk, vdisk.id
                            );
                            return Err(format!(
                                "cannot find in node: {:?}, disk with name: {:?} for vdisk: {:?}",
                                replica.node, replica.disk, vdisk.id
                            ));
                        }
                    }
                    None => {
                        debug!(
                            "cannot find node: {:?} for vdisk: {:?}",
                            replica.node, vdisk.id
                        );
                        return Err(format!(
                            "cannot find node: {:?} for vdisk: {:?}",
                            replica.node, vdisk.id
                        ));
                    }
                }
            }
        }

        Ok(())
    }
}

pub struct ClusterConfigYaml {}

impl ClusterConfigYaml {
    pub fn convert_to_data(&self, cluster: &ClusterConfig) -> Result<Vec<DataVDisk>, String> {
        let mut node_map: HashMap<&Option<String>, (&Node, HashMap<&Option<String>, String>)> =
            HashMap::new();
        for node in cluster.nodes.iter() {
            let disk_map = node
                .disks
                .iter()
                .map(|disk| (&disk.name, disk.path()))
                .collect::<HashMap<_, _>>();
            node_map.insert(&node.name, (node, disk_map));
        }

        let mut result: Vec<DataVDisk> = Vec::with_capacity(cluster.vdisks.len());
        for vdisk in cluster.vdisks.iter() {
            let mut disk = DataVDisk {
                id: VDiskId::new(vdisk.id() as u32),
                replicas: Vec::with_capacity(vdisk.replicas.len()),
            };
            for replica in vdisk.replicas.iter() {
                let finded_node = node_map.get(&replica.node).unwrap();
                let path = finded_node.1.get(&replica.disk).unwrap();

                let node_disk = DataNodeDisk {
                    path: path.to_string(),
                    name: replica.disk(),
                    node: DataNode {
                        name: replica.node(),
                        host: finded_node.0.host.borrow().to_string(),
                        port: finded_node.0.port.get(),
                    },
                };
                disk.replicas.push(node_disk);
            }
            result.push(disk);
        }
        Ok(result)
    }

    pub fn get(&self, filename: &str) -> Result<(Vec<DataVDisk>, ClusterConfig), String> {
        let config: ClusterConfig = YamlBobConfigReader {}.get::<ClusterConfig>(filename)?;
        match config.validate() {
            Ok(_) => Ok((self.convert_to_data(&config).unwrap(), config)),
            Err(e) => {
                debug!("config is not valid: {}", e);
                Err(format!("config is not valid: {}", e))
            }
        }
    }

    pub fn get_from_string(&self, file: &str) -> Result<(Vec<DataVDisk>, ClusterConfig), String> {
        let config: ClusterConfig = YamlBobConfigReader {}.parse(file)?;
        match config.validate() {
            Ok(_) => Ok((self.convert_to_data(&config).unwrap(), config)),
            Err(e) => {
                debug!("config is not valid: {}", e);
                Err(format!("config is not valid: {}", e))
            }
        }
    }
}
