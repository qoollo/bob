use itertools::Itertools;
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::HashMap;
use std::net::SocketAddr;

use crate::core::configs::reader::{BobConfigReader, Validatable, YamlBobConfigReader};
use crate::core::data::Node as DataNode;
use crate::core::data::NodeDisk as DataNodeDisk;
use crate::core::data::VDisk as DataVDisk;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeDisk {
    pub path: Option<String>,
    pub name: Option<String>,
}

impl Validatable for NodeDisk {
    fn validate(&self) -> Option<String> {
        if self.path.is_none() {
            debug!("field 'path' for 'disk' is invalid");
            return Some("field 'path' for 'disk' is invalid".to_string());
        }
        if self.path.as_ref()?.is_empty() {
            debug!("field 'path' for 'disk' is empty");
            return Some("field 'path' for 'disk' is empty".to_string());
        }
        if self.name.is_none() {
            debug!("field 'name' for 'disk' is invalid");
            return Some("field 'name' for 'disk' is invalid".to_string());
        }
        if self.name.as_ref()?.is_empty() {
            debug!("field 'name' for 'disk' is empty");
            return Some("field 'name' for 'disk' is empty".to_string());
        }
        None
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
    fn prepare(&self) -> Option<String> {
        let addr: Result<SocketAddr, _> = self.address.as_ref()?.parse();
        if addr.is_err() {
            debug!(
                "field 'address': {} for 'Node': {} is invalid",
                self.address.as_ref()?,
                self.name.as_ref()?
            );
            return Some(format!(
                "field 'address': {} for 'Node': {} is invalid",
                self.address.as_ref()?,
                self.name.as_ref()?
            ));
        }
        let ip = addr.unwrap();
        self.host.replace(ip.ip().to_string());
        self.port.set(ip.port());
        None
    }
}
impl Validatable for Node {
    fn validate(&self) -> Option<String> {
        if self.name.is_none() {
            debug!("field 'name' for 'Node' is invalid");
            return Some("field 'name' for 'Node' is invalid".to_string());
        }
        if self.name.as_ref()?.is_empty() {
            debug!("field 'name' for 'Node' is empty");
            return Some("field 'name' for 'Node' is empty".to_string());
        }
        if self.address.is_none() {
            debug!("field 'address' for 'Node' is invalid");
            return Some("field 'address' for 'Node' is invalid".to_string());
        }
        if self.address.as_ref()?.is_empty() {
            debug!("field 'address' for 'Node' is empty");
            return Some("field 'address' for 'Node' is empty".to_string());
        }

        let result = self.aggregate(&self.disks);
        if result.is_some() {
            debug!(
                "node is invalid: {} {}",
                self.name.as_ref()?,
                self.address.as_ref()?
            );
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
            debug!(
                "node: {} contains duplicate disk names",
                self.name.as_ref()?
            );
            return Some(format!(
                "node: {} contains duplicate disk names",
                self.name.as_ref()?
            ));
        }

        None
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Replica {
    pub node: Option<String>,
    pub disk: Option<String>,
}

impl Validatable for Replica {
    fn validate(&self) -> Option<String> {
        if self.node.is_none() {
            debug!("field 'node' for 'Replica' is invalid");
            return Some("field 'node' for 'Replica' is invalid".to_string());
        }
        if self.node.as_ref()?.is_empty() {
            debug!("field 'node' for 'Replica' is empty");
            return Some("field 'node' for 'Replica' is empty".to_string());
        }
        if self.disk.is_none() {
            debug!("field 'disk' for 'Replica' is invalid");
            return Some("field 'disk' for 'Replica' is invalid".to_string());
        }
        if self.disk.as_ref()?.is_empty() {
            debug!("field 'disk' for 'Replica' is empty");
            return Some("field 'disk' for 'Replica' is empty".to_string());
        }

        None
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct VDisk {
    pub id: Option<i32>,
    #[serde(default)]
    pub replicas: Vec<Replica>,
}

impl Validatable for VDisk {
    fn validate(&self) -> Option<String> {
        if self.id.is_none() {
            debug!("field 'id' for 'VDisk' is not set");
            return Some("field 'id' for 'VDisk' is not set".to_string());
        }

        if self.replicas.len() == 0 {
            debug!("vdisk must have replicas: {}", self.id.as_ref()?);
            return Some(format!("vdisk must have replicas: {}", self.id.as_ref()?));
        }
        let result = self.aggregate(&self.replicas);
        if result.is_some() {
            debug!("vdisk is invalid: {}", self.id.as_ref()?);
            return result;
        }

        if self
            .replicas
            .iter()
            .group_by(|x| x.clone())
            .into_iter()
            .map(|(_, group)| group.count())
            .filter(|x| *x > 1)
            .count()
            != 0
        {
            debug!("vdisk: {} contains duplicate replicas", self.id?);
            return Some(format!(
                "vdisk: {} contains duplicate replicas",
                self.id.as_ref()?
            ));
        }
        None
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Cluster {
    #[serde(default)]
    pub nodes: Vec<Node>,
    #[serde(default)]
    pub vdisks: Vec<VDisk>,
}

impl Validatable for Cluster {
    fn validate(&self) -> Option<String> {
        if self.nodes.len() == 0 {
            debug!("no nodes in config");
            return Some("no nodes in config".to_string());
        }
        if self.vdisks.len() == 0 {
            debug!("no vdisks in config");
            return Some("no vdisks in config".to_string());
        }
        let mut result = self.aggregate(&self.nodes);
        if result.is_some() {
            debug!("some nodes in config are invalid");
            return result;
        }
        result = self.aggregate(&self.vdisks);
        if result.is_some() {
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
            return Some("config contains duplicates vdisks ids".to_string());
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
            return Some("config contains duplicates nodes names".to_string());
        }

        let err = self
            .nodes
            .iter()
            .filter_map(|x| x.prepare())
            .fold("".to_string(), |acc, x| acc + "\n" + &x);
        if !err.is_empty() {
            return Some(err);
        }

        for vdisk in self.vdisks.iter() {
            for replica in vdisk.replicas.iter() {
                match self.nodes.iter().find(|x| x.name == replica.node) {
                    Some(node) => {
                        if node.disks.iter().find(|x| x.name == replica.disk) == None {
                            debug!(
                                "cannot find in node: {}, disk with name: {} for vdisk: {}",
                                replica.node.as_ref()?,
                                replica.disk.as_ref()?,
                                vdisk.id?
                            );
                            return Some(format!(
                                "cannot find in node: {}, disk with name: {} for vdisk: {}",
                                replica.node.as_ref()?,
                                replica.disk.as_ref()?,
                                vdisk.id?
                            ));
                        }
                    }
                    None => {
                        debug!(
                            "cannot find node: {} for vdisk: {}",
                            replica.node.as_ref()?,
                            vdisk.id?
                        );
                        return Some(format!(
                            "cannot find node: {} for vdisk: {}",
                            replica.node.as_ref()?,
                            vdisk.id?
                        ));
                    }
                }
            }
        }

        None
    }
}

pub trait BobClusterConfig {
    fn get(&self, filename: &str) -> Result<(Vec<DataVDisk>, Cluster), String>;
    fn convert_to_data(&self, cluster: &Cluster) -> Option<Vec<DataVDisk>>;
}

pub struct ClusterConfigYaml {}

impl BobClusterConfig for ClusterConfigYaml {
    fn convert_to_data(&self, cluster: &Cluster) -> Option<Vec<DataVDisk>> {
        let mut node_map: HashMap<&Option<String>, (&Node, HashMap<&Option<String>, String>)> =
            HashMap::new();
        for node in cluster.nodes.iter() {
            let disk_map = node
                .disks
                .iter()
                .map(|disk| (&disk.name, disk.path.as_ref().unwrap().clone()))
                .collect::<HashMap<_, _>>();
            node_map.insert(&node.name, (node, disk_map));
        }

        let mut result: Vec<DataVDisk> = Vec::with_capacity(cluster.vdisks.len());
        for vdisk in cluster.vdisks.iter() {
            let mut disk = DataVDisk {
                id: vdisk.id? as u32,
                replicas: Vec::with_capacity(vdisk.replicas.len()),
            };
            for replica in vdisk.replicas.iter() {
                let finded_node = node_map.get(&replica.node).unwrap();
                let path = finded_node.1.get(&replica.disk).unwrap();

                let node_disk = DataNodeDisk {
                    path: path.to_string(),
                    node: DataNode {
                        host: finded_node.0.host.borrow().to_string(),
                        port: finded_node.0.port.get(),
                    },
                };
                disk.replicas.push(node_disk);
            }
            result.push(disk);
        }
        Some(result)
    }
    fn get(&self, filename: &str) -> Result<(Vec<DataVDisk>, Cluster), String> {
        let config: Cluster = YamlBobConfigReader {}.get(filename)?;
        let is_valid = config.validate();
        if is_valid.is_some() {
            debug!("config is not valid: {}", is_valid.as_ref().unwrap());
            return Err(format!("config is not valid: {}", is_valid.unwrap()));
        }
        return Ok((self.convert_to_data(&config).unwrap(), config));
    }
}
