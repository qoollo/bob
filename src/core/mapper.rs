use super::prelude::*;

/// Struct for managing distribution of replicas on disks and nodes.
/// Through the virtual intermediate object, called `VDisk` - "virtual disk"
#[derive(Debug, Clone)]
pub struct Virtual {
    local_node_name: String,
    disks: Vec<DiskPath>,
    vdisks: Vec<DataVDisk>,
    nodes: Vec<Node>,
}

impl Virtual {
    /// Creates new instance of the Virtual disk mapper
    pub async fn new(vdisks: Vec<DataVDisk>, config: &NodeConfig, cluster: &ClusterConfig) -> Self {
        let (nodes, vdisks) = Self::prepare_nodes(vdisks, cluster).await;
        Self {
            local_node_name: config.name().to_owned(),
            disks: config.disks().clone(),
            vdisks,
            nodes,
        }
    }

    async fn prepare_nodes(
        mut vdisks: Vec<DataVDisk>,
        cluster: &ClusterConfig,
    ) -> (Vec<Node>, Vec<DataVDisk>) {
        let mut nodes = Vec::new();
        for (i, conf) in cluster.nodes().iter().enumerate() {
            let index = i.try_into().expect("usize to u16");
            let address = conf.address();
            nodes.push(Node::new(conf.name().to_owned(), address, index).await);
        }

        vdisks.iter_mut().for_each(|vdisk| vdisk.set_nodes(&nodes));
        (nodes, vdisks)
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

    pub(crate) fn get_vdisks_ids(&self) -> Vec<VDiskId> {
        self.vdisks.iter().map(VDisk::id).collect()
    }

    pub(crate) fn local_disks(&self) -> &[DiskPath] {
        &self.disks
    }

    pub(crate) fn vdisks(&self) -> &[DataVDisk] {
        &self.vdisks
    }

    pub(crate) fn get_disk(&self, name: &str) -> Option<&DiskPath> {
        self.disks.iter().find(|d| d.name() == name)
    }

    pub(crate) fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    pub(crate) fn id_from_key(&self, key: BobKey) -> VDiskId {
        (key % self.vdisks.len() as u64)
            .try_into()
            .expect("u64 to u32")
    }

    fn get_vdisk(&self, vdisk_id: VDiskId) -> &DataVDisk {
        self.vdisks
            .iter()
            .find(|disk| disk.id() == vdisk_id)
            .expect("find vdisk with id")
    }

    pub(crate) fn get_vdisk_for_key(&self, key: BobKey) -> &DataVDisk {
        let vdisk_id = self.id_from_key(key);
        self.get_vdisk(vdisk_id)
    }

    pub(crate) fn get_vdisks_by_disk(&self, disk: &str) -> Vec<VDiskId> {
        let vdisks = self.vdisks.iter();
        let vdisks_on_disk = vdisks.filter(|vdisk| {
            vdisk
                .replicas()
                .iter()
                .filter(|r| r.node_name() == self.local_node_name)
                .any(|replica| replica.disk_name() == disk)
        });
        vdisks_on_disk.map(VDisk::id).collect()
    }

    pub(crate) fn get_operation(&self, key: BobKey) -> (VDiskId, Option<DiskPath>) {
        let vdisk_id = self.id_from_key(key);
        let virt_disk = self.get_vdisk(vdisk_id);
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

    pub(crate) fn is_vdisk_on_node(&self, node_name: &str, id: VDiskId) -> bool {
        self.vdisks.iter().any(|vdisk| {
            vdisk.id() == id && vdisk.nodes().iter().any(|node| node.name() == node_name)
        })
    }
}
