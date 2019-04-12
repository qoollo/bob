use itertools::Itertools;
use std::fs;
use std::cell::Cell;
use std::cell::RefCell;

pub trait Validatable {
    fn validate(&self) -> Option<String>;

    fn aggregate<T: Validatable>(&self, elements: &Option<Vec<T>>) -> Option<String>{
        let options: Vec<Option<String>> = elements.as_ref()?.iter()
            .map(|elem|elem.validate())
            .filter(|f| f.is_some())
            .collect::<Vec<Option<String>>>();
        if options.len() > 0 {
            return Some(options.iter().fold("".to_string(), |acc, x| acc + &x.as_ref().unwrap()));
        }
        None
    }
}

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
    pub disks: Option<Vec<NodeDisk>>,

    #[serde(skip)]
    pub host: RefCell<String>,
    #[serde(skip)]
    pub port: Cell<i32>,
}

impl Node {
    fn prepare (&self)->Option<String> {
        let ip: Vec<&str> = self.address.as_ref()?.split(":").collect::<Vec<&str>>();
        self.host.replace(ip[0].to_string());
        let port = ip[1].parse::<i32>();
        if port.is_err(){
            debug!("cannot parse node: {} address: {}", self.name.as_ref()?, self.address.as_ref()?);
            return Some(format!("cannot parse node: {} address: {}", self.name.as_ref()?, self.address.as_ref()?));
        }
        self.port.set(port.unwrap());
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
            debug!("node is invalid: {} {}", self.name.as_ref()?, self.address.as_ref()?);
            return result;
        }

        if self.disks.as_ref()?
            .iter()
            .group_by(|x| x.name.clone())
            .into_iter()
            .map(|(_, group)| group.count())
            .filter(|x| *x > 1)
            .count() != 0 {
                debug!("node: {} contains duplicate disk names", self.name.as_ref()?);
                return Some(format!("node: {} contains duplicate disk names", self.name.as_ref()?))
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
    pub replicas: Option<Vec<Replica>>,
}

impl Validatable for VDisk {
    fn validate(&self) -> Option<String> {
        if self.id.is_none() {
            debug!("field 'id' for 'VDisk' is invalid");
            return Some("field 'id' for 'VDisk' is invalid".to_string());
        }

        let result = self.aggregate(&self.replicas);
        if result.is_some() {
            debug!("vdisk is invalid: {}", self.id.as_ref()?);
            return result;
        }

        if self.replicas.as_ref()?
            .iter()
            .group_by(|x| x.clone())
            .into_iter()
            .map(|(_, group)| group.count())
            .filter(|x| *x > 1)
            .count() != 0 {
                debug!("vdisk: {} contains duplicate replicas", self.id?);
                return Some(format!("vdisk: {} contains duplicate replicas", self.id.as_ref()?))
            }
        None
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Cluster {
    pub nodes: Option<Vec<Node>>,
    pub vdisks: Option<Vec<VDisk>>,
}

impl Validatable for Cluster {
    fn validate(&self) -> Option<String> {
        if self.nodes.is_none()
        {
            debug!("no nodes in config");
            return Some("no nodes in config".to_string());
        }
        if self.vdisks.is_none()
        {
            debug!("no vdisks in config");
            return Some("no vdisks in config".to_string()   );
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

        if self.vdisks.as_ref()?
            .iter()
            .group_by(|x| x.id)
            .into_iter()
            .map(|(_, group)| group.count())
            .filter(|x| *x > 1)
            .count() != 0 
        {
            debug!("config contains duplicates vdisks ids");
            return Some("config contains duplicates vdisks ids".to_string())
        }
        if self.nodes.as_ref()?
            .iter()
            .group_by(|x| x.name.clone())
            .into_iter()
            .map(|(_, group)| group.count())
            .filter(|x| *x > 1)
            .count() != 0 
        {
            debug!("config contains duplicates nodes names");
            return Some("config contains duplicates nodes names".to_string())
        }        

        for node in self.nodes.as_ref()?.iter() {
            node.prepare();
        }

        for vdisk in self.vdisks.as_ref()?.iter() {
            for replica in vdisk.replicas.as_ref()?.iter() {
                match self.nodes.as_ref()?.iter().find(|x| x.name == replica.node) {
                    Some(node) => {
                        if node.disks.as_ref()?.iter().find(|x| x.name == replica.disk) == None {
                            debug!(
                                "cannot find in node: {}, disk with name: {} for vdisk: {}",
                                replica.node.as_ref()?, replica.disk.as_ref()?, vdisk.id?
                            );
                            return Some(format!("cannot find in node: {}, disk with name: {} for vdisk: {}",
                                replica.node.as_ref()?, replica.disk.as_ref()?, vdisk.id?));
                        }
                    }
                    None => {
                        debug!("cannot find node: {} for vdisk: {}", replica.node.as_ref()?, vdisk.id?);
                        return Some(format!("cannot find node: {} for vdisk: {}", replica.node.as_ref()?, vdisk.id?));
                    }
                }
            }
        }

        None
    }
}

use crate::core::data::Node as DataNode;
use crate::core::data::NodeDisk as DataNodeDisk;
use crate::core::data::VDisk as DataVDisk;

use std::collections::HashMap;

pub trait BobConfig {
    fn get_cluster_config(&self, filename: &String) -> Result<Vec<DataVDisk>, String>;

    fn read_config(&self, filename: &String) -> Result<Cluster, String>;
    fn parse_config(&self, config: &String) -> Result<Cluster, String>;
    fn convert_to_data(&self, cluster: &Cluster) -> Option<Vec<DataVDisk>>;
}

pub struct YamlConfig {}

impl BobConfig for YamlConfig {
    fn read_config(&self, filename: &String) -> Result<Cluster, String> {
        let result: Result<String, _> = fs::read_to_string(filename);
        match result {
            Ok(config) => return self.parse_config(&config),
            Err(e) => {
                debug!("error on file opening: {}", e);
                return Err(format!("error on file opening: {}", e));
            }
        }
        
    }
    fn parse_config(&self, config: &String) -> Result<Cluster, String> {
        let result: Result<Cluster, _> = serde_yaml::from_str(config);
        match result {
            Ok(cluster) => return Ok(cluster),
            Err(e) => {
                debug!("error on yaml parsing: {}", e);
                return Err(format!("error on yaml parsing: {}", e));
            }
        }
    }
    fn convert_to_data(&self, cluster: &Cluster) -> Option<Vec<DataVDisk>> {
        let mut node_map: HashMap<&Option<String>, (&Node, HashMap<&Option<String>, String>)> = HashMap::new();
        for node in cluster.nodes.as_ref()?.iter() {
            let mut disk_map = HashMap::new();
            for disk in node.disks.as_ref()?.iter() {
                disk_map.insert(&disk.name, disk.path.as_ref()?.clone());
            }
            node_map.insert(&node.name, (node, disk_map));
        }

        let mut result: Vec<DataVDisk> = Vec::with_capacity(cluster.vdisks.as_ref()?.len());
        for vdisk in cluster.vdisks.as_ref()?.iter() {
            let mut disk = DataVDisk {
                id: vdisk.id? as u32,
                replicas: Vec::with_capacity(vdisk.replicas.as_ref()?.len()),
            };
            for replica in vdisk.replicas.as_ref()?.iter() {
                let finded_node = node_map.get(&replica.node).unwrap();
                let path = finded_node.1.get(&replica.disk).unwrap();

                let node_disk = DataNodeDisk {
                    path: path.to_string(),
                    node: DataNode {
                        host: finded_node.0.host.borrow().to_string(),
                        port: finded_node.0.port.get() as u16,
                    },
                };
                disk.replicas.push(node_disk);
            }
            result.push(disk);
        }
        Some(result)
    }
    fn get_cluster_config(&self, filename: &String) -> Result<Vec<DataVDisk>, String> {
        let file = self.read_config(filename);
        match file {
            Ok(config)=>{
                let is_valid = config.validate();
                if is_valid.is_some() {
                    debug!("config is not valid: {}", is_valid.as_ref().unwrap());
                    return Err(format!("config is not valid: {}", is_valid.unwrap()));
                }
                return Ok(self.convert_to_data(&config).unwrap());
            },
            _ => return Err("".to_string()),
        }
    }
}
