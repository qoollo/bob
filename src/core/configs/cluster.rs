use super::prelude::*;

/// Structure represents disk on the node. Contains path to disk and name.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeDisk {
    path: Option<String>,
    name: Option<String>,
}

impl NodeDisk {
    /// Returns disk name, panics if no name was set in config.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_ref().expect("node disk name")
    }

    /// Returns disk path, panics if no path was set in config.
    #[must_use]
    pub fn path(&self) -> &str {
        self.path.as_ref().expect("node disk path")
    }
}

impl Validatable for NodeDisk {
    fn validate(&self) -> Result<(), String> {
        match &self.name {
            None => {
                debug!("field 'name' for 'disk' is not set");
                Err("field 'name' for 'disk' is not set".to_string())
            }
            Some(name) => {
                if name.is_empty() {
                    debug!("field 'name' for 'disk' is empty");
                    Err("field 'name' for 'disk' is empty".to_string())
                } else {
                    Ok(())
                }
            }
        }?;
        match &self.path {
            None => {
                debug!("field 'path' for 'disk' is not set");
                Err("field 'path' for 'disk' is not set".to_string())
            }
            Some(path) => {
                if path.is_empty() {
                    debug!("field 'path' for 'disk' is empty");
                    Err("field 'path' for 'disk' is empty".to_string())
                } else {
                    Ok(())
                }
            }
        }
    }
}

/// Node config struct, with name, address and [`NodeDisk`]s.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Node {
    name: Option<String>,
    address: Option<String>,
    #[serde(default)]
    disks: Vec<NodeDisk>,

    #[serde(skip)]
    host: RefCell<String>,
    #[serde(skip)]
    port: Cell<u16>,
}

impl Node {
    /// Returns node name, panics if no name was set in config.
    #[inline]
    pub fn name(&self) -> &str {
        self.name.as_ref().expect("node name")
    }

    #[inline]
    pub fn disks(&self) -> &[NodeDisk] {
        &self.disks
    }

    #[inline]
    pub fn address(&self) -> String {
        self.address.clone().expect("clone address")
    }

    #[inline]
    pub(crate) fn host(&self) -> String {
        self.host.borrow().clone()
    }

    #[inline]
    pub(crate) fn port(&self) -> u16 {
        self.port.get()
    }

    fn prepare(&self) -> Result<(), String> {
        self.address()
            .parse::<SocketAddr>()
            .map(|ip| {
                self.host.replace(ip.ip().to_string());
                self.port.set(ip.port());
            })
            .map_err(|_| {
                debug!(
                    "field 'address': {} for 'Node': {} is invalid",
                    self.address(),
                    self.name()
                );
                format!(
                    "field 'address': {} for 'Node': {} is invalid",
                    self.address(),
                    self.name()
                )
            })
    }
}
impl Validatable for Node {
    fn validate(&self) -> Result<(), String> {
        match &self.name {
            None => {
                debug!("field 'name' for 'Node' is not set");
                Err("field 'name' for 'Node' is not set".to_string())
            }
            Some(name) => {
                if name.is_empty() {
                    debug!("field 'name' for 'Node' is empty");
                    Err("field 'name' for 'Node' is empty".to_string())
                } else {
                    Ok(())
                }
            }
        }?;
        match &self.address {
            None => {
                debug!("field 'address' for 'Node' is not set");
                Err("field 'address' for 'Node' is not set".to_string())
            }
            Some(address) => {
                if address.is_empty() {
                    debug!("field 'address' for 'Node' is empty");
                    Err("field 'address' for 'Node' is empty".to_string())
                } else {
                    Ok(())
                }
            }
        }?;

        self.aggregate(&self.disks)?;

        let mut names = self
            .disks
            .iter()
            .filter_map(|disk| disk.name.as_ref())
            .collect::<Vec<_>>();
        names.sort();
        if names.windows(2).any(|pair| pair[0] == pair[1]) {
            debug!("node: {} contains duplicate disk names", self.name());
            Err(format!(
                "node: {} contains duplicate disk names",
                self.name()
            ))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Eq, PartialOrd, Ord, Clone)]
pub struct Replica {
    pub node: Option<String>,
    pub disk: Option<String>,
}

impl Replica {
    #[must_use]
    pub fn node(&self) -> &str {
        self.node.as_ref().expect("replica node")
    }

    #[must_use]
    fn disk(&self) -> &str {
        self.disk.as_ref().expect("replica disk")
    }
}
impl Validatable for Replica {
    fn validate(&self) -> Result<(), String> {
        match &self.node {
            None => {
                debug!("field 'node' for 'Replica' is not set");
                Err("field 'node' for 'Replica' is not set".to_string())
            }
            Some(node) => {
                if node.is_empty() {
                    debug!("field 'node' for 'Replica' is empty");
                    Err("field 'node' for 'Replica' is empty".to_string())
                } else {
                    Ok(())
                }
            }
        }?;
        match &self.disk {
            None => {
                debug!("field 'disk' for 'Replica' is not set");
                Err("field 'disk' for 'Replica' is not set".to_string())
            }
            Some(disk) => {
                if disk.is_empty() {
                    debug!("field 'disk' for 'Replica' is empty");
                    Err("field 'disk' for 'Replica' is empty".to_string())
                } else {
                    Ok(())
                }
            }
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct VDisk {
    id: Option<i32>,
    #[serde(default)]
    replicas: Vec<Replica>,
}

impl VDisk {
    pub fn new(id: i32) -> Self {
        Self {
            id: Some(id),
            replicas: Vec::new(),
        }
    }

    #[must_use]
    pub fn id(&self) -> i32 {
        self.id.expect("VDisk id")
    }

    pub fn replicas(&self) -> &[Replica] {
        &self.replicas
    }

    pub fn push_replica(&mut self, replica: Replica) {
        self.replicas.push(replica)
    }
}

impl Validatable for VDisk {
    fn validate(&self) -> Result<(), String> {
        self.id.ok_or_else(|| {
            debug!("field 'id' for 'VDisk' is not set");
            "field 'id' for 'VDisk' is not set".to_string()
        })?;

        if self.replicas.is_empty() {
            debug!("vdisk must have replicas: {}", self.id());
            return Err(format!("vdisk must have replicas: {}", self.id()));
        }
        self.aggregate(&self.replicas).map_err(|e| {
            debug!("vdisk is invalid: {}", self.id());
            e
        })?;

        let mut replicas_ref = self.replicas.iter().collect::<Vec<_>>();
        replicas_ref.sort();
        if replicas_ref.windows(2).any(|pair| pair[0] == pair[1]) {
            debug!("vdisk: {} contains duplicate replicas", self.id());
            Err(format!("vdisk: {} contains duplicate replicas", self.id()))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub nodes: Vec<Node>,
    #[serde(default)]
    pub vdisks: Vec<VDisk>,
}

impl Validatable for Config {
    fn validate(&self) -> Result<(), String> {
        if self.nodes.is_empty() {
            debug!("no nodes in config");
            return Err("no nodes in config".to_string());
        }
        if self.vdisks.is_empty() {
            debug!("no vdisks in config");
            return Err("no vdisks in config".to_string());
        }
        self.aggregate(&self.nodes).map_err(|e| {
            debug!("some nodes in config are invalid");
            e
        })?;
        self.aggregate(&self.vdisks).map_err(|e| {
            debug!("some vdisks in config are invalid");
            e
        })?;

        let mut vdisks_id = self.vdisks.iter().map(|vdisk| vdisk.id).collect::<Vec<_>>();
        vdisks_id.sort();
        if vdisks_id.windows(2).any(|pair| pair[0] == pair[1]) {
            debug!("config contains duplicates vdisks ids");
            return Err("config contains duplicates vdisks ids".to_string());
        }

        let mut node_names = self
            .nodes
            .iter()
            .filter_map(|node| node.name.as_ref())
            .collect::<Vec<_>>();
        node_names.sort();
        if node_names.windows(2).any(|pair| pair[0] == pair[1]) {
            debug!("config contains duplicates nodes names");
            return Err("config contains duplicates nodes names".to_string());
        }

        let err = self
            .nodes
            .iter()
            .filter_map(|x| x.prepare().err())
            .fold(String::new(), |acc, x| acc + "\n" + &x);
        if !err.is_empty() {
            return Err(err);
        }

        for vdisk in &self.vdisks {
            for replica in &vdisk.replicas {
                if let Some(node) = self.nodes.iter().find(|x| x.name == replica.node) {
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
                } else {
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

        Ok(())
    }
}

pub struct ConfigYaml {}

impl ConfigYaml {
    pub(crate) fn convert(cluster: &Config) -> Result<Vec<DataVDisk>, String> {
        let mut node_map = HashMap::new();
        for node in &cluster.nodes {
            let disk_map = node
                .disks
                .iter()
                .map(|disk| (&disk.name, disk.path()))
                .collect::<HashMap<_, _>>();
            node_map.insert(&node.name, (node, disk_map));
        }

        let mut result: Vec<DataVDisk> = Vec::with_capacity(cluster.vdisks.len());
        for vdisk in &cluster.vdisks {
            let mut disk = DataVDisk::new(vdisk.id() as u32, vdisk.replicas.len());

            for replica in &vdisk.replicas {
                let finded_node = node_map.get(&replica.node).expect("get replica node");
                let node_name = finded_node.0.name().to_owned();
                let path = (*finded_node.1.get(&replica.disk).expect("get disk")).to_string();

                let node_disk = DataNodeDisk::new(path, replica.disk().to_string(), node_name);
                disk.push_replica(node_disk);
            }
            result.push(disk);
        }
        Ok(result)
    }

    pub(crate) fn convert_to_data(cluster: &Config) -> Result<Vec<DataVDisk>, String> {
        Self::convert(cluster)
    }

    pub fn get(filename: &str) -> Result<(Vec<DataVDisk>, Config), String> {
        let config = YamlBobConfigReader::get::<Config>(filename)?;
        match config.validate() {
            Ok(_) => Ok((
                Self::convert_to_data(&config).expect("convert config to data"),
                config,
            )),
            Err(e) => {
                debug!("config is not valid: {}", e);
                Err(format!("config is not valid: {}", e))
            }
        }
    }

    pub(crate) fn get_from_string(file: &str) -> Result<(Vec<DataVDisk>, Config), String> {
        let config = YamlBobConfigReader::parse::<Config>(file)?;
        debug!("config: {:?}", config);
        if let Err(e) = config.validate() {
            debug!("config is not valid: {}", e);
            Err(format!("config is not valid: {}", e))
        } else {
            debug!("config is valid");
            Ok((
                Self::convert_to_data(&config).expect("convert config to data"),
                config,
            ))
        }
    }
}

pub(crate) mod tests {
    use super::*;

    #[must_use]
    pub(crate) fn cluster_config(count_nodes: u8, count_vdisks: u8, count_replicas: u8) -> Config {
        let nodes = (0..count_nodes)
            .map(|id| {
                let name = id.to_string();
                Node {
                    name: Some(name.clone()),
                    address: Some("1".to_string()),
                    disks: vec![NodeDisk {
                        name: Some(name.clone()),
                        path: Some(name),
                    }],
                    host: RefCell::default(),
                    port: Cell::default(),
                }
            })
            .collect();

        let vdisks = (0..count_vdisks)
            .map(|id| {
                let replicas = (0..count_replicas)
                    .map(|r| {
                        let n = ((id + r) % count_nodes).to_string();
                        Replica {
                            node: Some(n.clone()),
                            disk: Some(n),
                        }
                    })
                    .collect();
                VDisk {
                    id: Some(i32::from(id)),
                    replicas,
                }
            })
            .collect();

        Config { nodes, vdisks }
    }
}
