use super::prelude::*;

/// Hash map with IDs as keys and `VDisk`s as values.
pub type VDiskMap = HashMap<VDiskID, DataVDisk>;

/// Struct for managing distribution of replicas on disks and nodes.
/// Through the virtual intermediate object, called `VDisk` - "virtual disk"
#[derive(Debug, Clone)]
pub struct Virtual {
    local_node_name: String,
    disks: Vec<DiskPath>,
    vdisks: VDiskMap,
    nodes: Vec<Node>,
}

impl Virtual {
    /// Creates new instance of the Virtual disk mapper
    pub async fn new(config: &NodeConfig, cluster: &ClusterConfig) -> Self {
        let mut vdisks = cluster.create_vdisks_map().unwrap();
        let nodes = Self::prepare_nodes(&mut vdisks, cluster).await;
        Self {
            local_node_name: config.name().to_owned(),
            disks: config.disks().clone(),
            vdisks,
            nodes,
        }
    }

    async fn prepare_nodes(vdisks: &mut VDiskMap, cluster: &ClusterConfig) -> Vec<Node> {
        let mut nodes = Vec::new();
        for (i, conf) in cluster.nodes().iter().enumerate() {
            let index = i.try_into().expect("usize to u16");
            let address = conf.address();
            nodes.push(Node::new(conf.name().to_owned(), address, index).await);
        }

        vdisks
            .values_mut()
            .for_each(|vdisk| vdisk.set_nodes(&nodes));
        nodes
    }

    pub(crate) fn local_node_name(&self) -> &str {
        &self.local_node_name
    }

    pub(crate) fn local_node_address(&self) -> String {
        let name = self.local_node_name();
        self.nodes
            .iter()
            .find(|node| node.name() == name)
            .expect("found node with name")
            .address()
            .to_string()
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

    pub(crate) fn vdisks(&self) -> &VDiskMap {
        &self.vdisks
    }

    pub(crate) fn get_disk(&self, name: &str) -> Option<&DiskPath> {
        self.disks.iter().find(|d| d.name() == name)
    }

    pub(crate) fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    pub(crate) fn vdisk_id_from_key(&self, key: BobKey) -> VDiskID {
        (key % self.vdisks.len() as u64)
            .try_into()
            .expect("u64 to u32")
    }

    /// Returns ref to vdisk by given id.
    #[must_use]
    pub fn get_vdisk(&self, id: VDiskID) -> Option<&DataVDisk> {
        debug!("mapper virtual get vdisk with id: {}", id);
        self.vdisks.get(&id)
    }

    pub(crate) fn get_vdisk_for_key(&self, key: BobKey) -> Option<&DataVDisk> {
        let vdisk_id = self.vdisk_id_from_key(key);
        self.get_vdisk(vdisk_id)
    }

    pub(crate) fn get_vdisks_by_disk(&self, disk: &str) -> Vec<VDiskID> {
        self.vdisks
            .iter()
            .filter_map(|(id, vd)| {
                if vd
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
        let vdisk_id = self.vdisk_id_from_key(key);
        let virt_disk = self.get_vdisk(vdisk_id).expect("vdisk not found");
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
                self.local_node_name, vdisk_id
            );
        }
        (vdisk_id, disk)
    }

    pub(crate) fn is_vdisk_on_node(&self, node_name: &str, id: VDiskID) -> bool {
        self.vdisks
            .get(&id)
            .expect("vdisk not found")
            .nodes()
            .iter()
            .any(|node| node.name() == node_name)
    }
}
