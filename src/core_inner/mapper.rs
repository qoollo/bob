use super::prelude::*;

#[derive(Debug, Clone)]
pub struct VDiskMapper {
    local_node_name: String,
    disks: Vec<DiskPath>,
    vdisks: Vec<DataVDisk>,
    nodes: Vec<Node>,
}

impl VDiskMapper {
    pub fn new(
        vdisks: Vec<DataVDisk>,
        config: &NodeConfig,
        cluster: &ClusterConfig,
    ) -> VDiskMapper {
        VDiskMapper::new_direct(
            vdisks,
            &config.name.as_ref().unwrap().to_string(),
            &config.disks(),
            cluster,
        )
    }
    pub fn new_direct(
        vdisks: Vec<DataVDisk>,
        node_name: &str,
        disks: &[ConfigDiskPath],
        cluster: &ClusterConfig,
    ) -> VDiskMapper {
        let (nodes, vdisks) = Self::prepare_nodes(vdisks, &cluster);

        VDiskMapper {
            vdisks,
            local_node_name: node_name.to_string(),
            disks: disks
                .iter()
                .map(|d| DiskPath::new(&d.name.clone(), &d.path.clone()))
                .collect(),
            nodes,
        }
    }
    fn prepare_nodes(
        mut vdisks: Vec<DataVDisk>,
        cluster: &ClusterConfig,
    ) -> (Vec<Node>, Vec<DataVDisk>) {
        let mut index = 0;
        let nodes: Vec<_> = cluster
            .nodes
            .iter()
            .map(|node| {
                let mut n = Node::from(node);
                n.index = index;
                index += 1;
                n
            })
            .collect();

        vdisks.iter_mut().for_each(|vdisk| vdisk.set_nodes(&nodes));
        (nodes, vdisks.to_vec())
    }

    pub fn local_node_name(&self) -> String {
        self.local_node_name.clone()
    }
    pub fn vdisks_count(&self) -> u32 {
        self.vdisks.len() as u32
    }

    pub fn get_vdisks_ids(&self) -> Vec<VDiskId> {
        self.vdisks.iter().map(|vdisk| vdisk.id.clone()).collect()
    }

    pub fn local_disks(&self) -> &Vec<DiskPath> {
        &self.disks
    }
    pub fn get_disk_by_name(&self, name: &str) -> Option<&DiskPath> {
        self.disks.iter().find(|d| d.name == name)
    }
    pub fn nodes(&self) -> &Vec<Node> {
        &self.nodes //TODO
    }

    pub fn get_vdisk(&self, key: BobKey) -> &DataVDisk {
        let vdisk_id = VDiskId::new((key.key % self.vdisks.len() as u64) as u32);
        self.vdisks.iter().find(|disk| disk.id == vdisk_id).unwrap()
    }

    pub fn get_vdisks_by_disk(&self, disk: &str) -> Vec<VDiskId> {
        self.vdisks
            .iter()
            .filter(|vdisk| {
                vdisk.replicas.iter().any(|replica| {
                    replica.node_name == self.local_node_name && replica.disk_name == disk
                })
            })
            .map(|vdisk| vdisk.id.clone())
            .collect()
    }

    pub fn get_operation(&self, key: BobKey) -> (VDiskId, Option<DiskPath>) {
        let vdisk_id = VDiskId::new((key.key % self.vdisks.len() as u64) as u32);
        let vdisk = self.vdisks.iter().find(|disk| disk.id == vdisk_id).unwrap();
        let disk = vdisk
            .replicas
            .iter()
            .find(|disk| disk.node_name == self.local_node_name); //TODO prepare at start?
        if disk.is_none() {
            trace!(
                "cannot find node: {} for vdisk: {}",
                self.local_node_name,
                vdisk_id
            );
            return (vdisk_id, None);
        }
        (vdisk_id, Some(DiskPath::from(disk.unwrap())))
    }

    pub fn does_node_holds_vdisk(&self, node_name: &str, id: VDiskId) -> bool {
        self.vdisks
            .iter()
            .any(|vdisk| vdisk.id == id && vdisk.nodes.iter().any(|node| node.name() == node_name))
    }
}
