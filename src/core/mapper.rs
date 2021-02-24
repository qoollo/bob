use super::prelude::*;

/// Hash map with IDs as keys and `VDisk`s as values.
pub type VDisksMap = HashMap<VDiskID, DataVDisk>;

pub(crate) type NodesMap = HashMap<NodeID, Node>;

type SemMap = HashMap<String, Arc<Semaphore>>;

/// Struct for managing distribution of replicas on disks and nodes.
/// Through the virtual intermediate object, called `VDisk` - "virtual disk"
#[derive(Debug, Clone)]
pub struct Virtual {
    local_node_name: String,
    local_node_address: String,
    disks: Vec<DiskPath>,
    vdisks: VDisksMap,
    nodes: NodesMap,
    distribution_func: DistributionFunc,
    disk_access_sems: Arc<RwLock<SemMap>>,
    disk_access_par_degree: usize,
}

impl Virtual {
    /// Creates new instance of the Virtual disk mapper
    pub async fn new(config: &NodeConfig, cluster: &ClusterConfig) -> Self {
        let mut vdisks = cluster.create_vdisks_map().unwrap();
        let nodes = Self::prepare_nodes(&mut vdisks, cluster).await;
        let local_node_name = config.name().to_owned();
        let local_node_address = nodes
            .values()
            .find(|node| *node.name() == local_node_name)
            .expect("found node with name")
            .address()
            .to_string();
        Self {
            local_node_name,
            local_node_address,
            disks: config.disks().clone(),
            vdisks,
            nodes,
            distribution_func: cluster.distribution_func(),
            disk_access_sems: Arc::new(RwLock::new(SemMap::new())),
            disk_access_par_degree: config.init_par_degree(),
        }
    }

    async fn prepare_nodes(vdisks: &mut VDisksMap, cluster: &ClusterConfig) -> NodesMap {
        let nodes = cluster
            .nodes()
            .iter()
            .enumerate()
            .map(|(i, conf)| {
                let index = i.try_into().expect("usize to u16");
                let address = conf.address();
                let name = conf.name().to_owned();
                async move {
                    let node = Node::new(name, address, index).await;
                    (index, node)
                }
            })
            .collect::<FuturesUnordered<_>>()
            .collect()
            .await;

        vdisks
            .values_mut()
            .for_each(|vdisk| vdisk.set_nodes(&nodes));
        nodes
    }

    pub(crate) fn local_node_name(&self) -> &str {
        &self.local_node_name
    }

    pub(crate) fn local_node_address(&self) -> &str {
        &self.local_node_address
    }

    pub(crate) fn vdisks_count(&self) -> u32 {
        self.vdisks.len().try_into().expect("usize to u32")
    }

    pub(crate) fn get_vdisks_ids(&self) -> Vec<VDiskID> {
        self.vdisks.keys().copied().collect()
    }

    pub(crate) fn local_disks(&self) -> &[DiskPath] {
        &self.disks
    }

    pub(crate) fn vdisks(&self) -> &VDisksMap {
        &self.vdisks
    }

    pub(crate) fn get_disk(&self, name: &str) -> Option<&DiskPath> {
        self.disks.iter().find(|d| d.name() == name)
    }

    pub(crate) fn nodes(&self) -> &HashMap<NodeID, Node> {
        &self.nodes
    }

    pub(crate) fn distribution_func(&self) -> DistributionFunc {
        self.distribution_func
    }

    pub(crate) async fn get_disk_access_sem(&self, disk_name: &str) -> Arc<Semaphore> {
        {
            let read = self.disk_access_sems.read().await;
            if let Some(sem) = read.get(disk_name) {
                return sem.clone();
            }
        }
        let mut write = self.disk_access_sems.write().await;
        if let Some(sem) = write.get(disk_name) {
            return sem.clone();
        }
        let sem = Arc::new(Semaphore::new(self.disk_access_par_degree));
        write.insert(disk_name.to_owned(), sem.clone());
        sem
    }

    pub(crate) fn get_target_nodes_for_key(&self, key: BobKey) -> &[Node] {
        let id = self.vdisk_id_from_key(key);
        self.vdisks.get(&id).expect("vdisk not found").nodes()
    }

    pub(crate) fn get_support_nodes(&self, key: BobKey, count: usize) -> Vec<&Node> {
        trace!("get target nodes for given key");
        let target_nodes = self.get_target_nodes_for_key(key);
        trace!("extract indexes of target nodes");
        let mut target_indexes = target_nodes.iter().map(Node::index);
        let len = target_indexes.size_hint().0;
        debug!("iterator size lower bound: {}", len);
        trace!("nodes available: {}", self.nodes.len());
        self.nodes
            .iter()
            .filter_map(|(id, node)| {
                if target_indexes.all(|i| &i != id) {
                    Some(node)
                } else {
                    None
                }
            })
            .take(count)
            .collect()
    }

    pub(crate) fn vdisk_id_from_key(&self, key: BobKey) -> VDiskID {
        match self.distribution_func {
            DistributionFunc::Mod => (key % self.vdisks.len() as u64)
                .try_into()
                .expect("u64 to u32"),
        }
    }

    /// Returns ref to `VDisk` with given ID
    #[must_use]
    pub fn get_vdisk(&self, vdisk_id: VDiskID) -> Option<&DataVDisk> {
        self.vdisks.get(&vdisk_id)
    }

    pub(crate) fn get_vdisk_for_key(&self, key: BobKey) -> Option<&DataVDisk> {
        let vdisk_id = self.vdisk_id_from_key(key);
        self.get_vdisk(vdisk_id)
    }

    pub(crate) fn get_vdisks_by_disk(&self, disk: &str) -> Vec<VDiskID> {
        let vdisks = self.vdisks.iter();
        vdisks
            .filter_map(|(id, vdisk)| {
                if vdisk
                    .replicas()
                    .iter()
                    .filter(|r| r.node_name() == self.local_node_name)
                    .any(|replica| replica.disk_name() == disk)
                {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect()
    }

    pub(crate) fn get_operation(&self, key: BobKey) -> (VDiskID, Option<DiskPath>) {
        let virt_disk = self.get_vdisk_for_key(key).expect("vdisk not found");
        let disk = virt_disk.replicas().iter().find_map(|disk| {
            if disk.node_name() == self.local_node_name {
                Some(DiskPath::from(disk))
            } else {
                None
            }
        }); //TODO prepare at start?
        if disk.is_none() {
            debug!(
                "cannot find node: {} for vdisk: {}",
                self.local_node_name,
                virt_disk.id()
            );
        }
        (virt_disk.id(), disk)
    }

    pub(crate) fn is_vdisk_on_node(&self, node_name: &str, id: VDiskID) -> bool {
        self.get_vdisk(id)
            .expect("vdisk not found")
            .nodes()
            .iter()
            .any(|node| node.name() == node_name)
    }
}
