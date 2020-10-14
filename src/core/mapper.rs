use super::{data::NodeID, prelude::*};

/// Struct for managing distribution of replicas on disks and nodes.
/// Through the virtual intermediate object, called `VDisk` - "virtual disk"
#[derive(Debug, Clone)]
pub struct Virtual {
    local_node_name: String,
    local_node_address: String,
    disks: Vec<DiskPath>,
    vdisks: HashMap<VDiskID, DataVDisk>,
    nodes: HashMap<NodeID, Node>,
}

impl Virtual {
    /// Creates new instance of the Virtual disk mapper
    pub async fn new(
        vdisks: HashMap<VDiskID, DataVDisk>,
        config: &NodeConfig,
        cluster: &ClusterConfig,
    ) -> Self {
        let (nodes, vdisks) = Self::prepare_nodes(vdisks, cluster).await;
        let local_node_name = config.name().to_owned();
        let local_node_address = cluster
            .nodes()
            .iter()
            .find(|conf| conf.name() == &local_node_name)
            .expect("not found local node in cluster conifg")
            .name()
            .to_owned();
        Self {
            local_node_address,
            local_node_name,
            disks: config.disks().clone(),
            vdisks,
            nodes,
        }
    }

    async fn prepare_nodes(
        mut vdisks: HashMap<VDiskID, DataVDisk>,
        cluster: &ClusterConfig,
    ) -> (HashMap<NodeID, Node>, HashMap<VDiskID, DataVDisk>) {
        let nodes = cluster
            .nodes()
            .iter()
            .enumerate()
            .map(|(i, conf)| {
                let index = i.try_into().expect("usize to u16");
                Node::new(conf.name().to_owned(), conf.address(), index)
                    .map(move |node| (index, node))
            })
            .collect::<FuturesUnordered<_>>()
            .collect()
            .await;
        vdisks
            .values_mut()
            .for_each(|vdisk| vdisk.set_nodes(&nodes));
        (nodes, vdisks)
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

    pub(crate) fn vdisks(&self) -> Vec<&DataVDisk> {
        self.vdisks.values().collect()
    }

    pub(crate) fn get_disk(&self, name: &str) -> Option<&DiskPath> {
        self.disks.iter().find(|d| d.name() == name)
    }

    pub fn has_any_node_with_name(&self, name: &str) -> bool {
        self.nodes.values().any(|node| node.name() == name)
    }

    pub(crate) fn nodes(&self) -> &HashMap<NodeID, Node> {
        &self.nodes
    }

    pub(crate) fn get_remote_nodes(&self) -> impl Iterator<Item = &Node> {
        self.nodes
            .values()
            .filter(move |node| node.name() == &self.local_node_name)
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
        (key % self.vdisks.len() as u64)
            .try_into()
            .expect("u64 to u32")
    }

    pub fn get_vdisk(&self, vdisk_id: &VDiskID) -> Option<&DataVDisk> {
        self.vdisks.get(vdisk_id)
    }

    pub(crate) fn get_vdisk_for_key(&self, key: BobKey) -> &DataVDisk {
        let vdisk_id = self.vdisk_id_from_key(key);
        self.get_vdisk(&vdisk_id).expect("vdisk id not found")
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
        let virt_disk = self.get_vdisk_for_key(key);
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
        self.get_vdisk(&id)
            .expect("vdisk not found")
            .nodes()
            .iter()
            .any(|node| node.name() == node_name)
    }
}
