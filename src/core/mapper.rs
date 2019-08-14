use crate::core::{
    backend::core::BackendOperation,
    configs::node::{DiskPath as ConfigDiskPath, NodeConfig},
    configs::cluster::ClusterConfig,
    data::*,
};

#[derive(Debug, Clone)]
pub struct VDiskMapper {
    local_node_name: String,
    disks: Vec<DiskPath>,
    vdisks: Vec<VDisk>,
    nodes: Vec<Node>,
}

impl VDiskMapper {
    pub fn new(vdisks: Vec<VDisk>, config: &NodeConfig, cluster: &ClusterConfig) -> VDiskMapper {
        let nodes = Self::prepare_nodes_config(&vdisks, &cluster);
        // nodes = Self::link_nodes(nodes);

        VDiskMapper {
            vdisks,
            local_node_name: config.name.as_ref().unwrap().to_string(),
            disks: config
                .disks()
                .iter()
                .map(|d| DiskPath::new(&d.name.clone(), &d.path.clone()))
                .collect(),
            nodes,
        }
    }
    pub fn new2(vdisks: Vec<VDisk>, node_name: &str, disks: &[ConfigDiskPath]) -> VDiskMapper {
        let nodes = Self::prepare_nodes(&vdisks, disks);
        // nodes = Self::link_nodes(nodes);

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
    fn prepare_nodes_config(vdisks: &Vec<VDisk>, cluster: &ClusterConfig) -> Vec<Node> {
        let mut saved_nodes: Vec<Node> = vdisks
            .iter()
            .flat_map(|vdisk| vdisk.replicas.iter().map(|nd| nd.node.clone()))
            .collect();
        saved_nodes.dedup();

        cluster.nodes
            .iter()
            .map(|n| saved_nodes.iter().find(|node| node.name == n.name()).unwrap().clone())
            .collect()
    }
    fn prepare_nodes(vdisks: &Vec<VDisk>, disks: &[ConfigDiskPath]) -> Vec<Node> {
        let mut saved_nodes: Vec<Node> = vdisks
            .iter()
            .flat_map(|vdisk| vdisk.replicas.iter().map(|nd| nd.node.clone()))
            .collect();
        saved_nodes.dedup();

        disks
            .iter()
            .map(|n| saved_nodes.iter().find(|node| node.name == n.name).unwrap().clone())
            .collect()
    }

    pub fn vdisks_count(&self) -> u32 {
        self.vdisks.len() as u32
    }

    pub fn local_disks(&self) -> &Vec<DiskPath> {
        &self.disks
    }
    pub fn get_disk_by_name(&self, name: &str) -> Option<&DiskPath> {
        self.disks.iter().find(|d| d.name == name)
    }
    pub fn nodes(&self) -> Vec<Node> {
        self.nodes.clone()          //TODO
    }

    pub fn get_vdisk(&self, key: BobKey) -> &VDisk {
        let vdisk_id = VDiskId::new((key.key % self.vdisks.len() as u64) as u32);
        self.vdisks.iter().find(|disk| disk.id == vdisk_id).unwrap()
    }

    pub fn get_vdisks_by_disk(&self, disk: &str) -> Vec<VDiskId> {
        self.vdisks
            .iter()
            .filter(|vdisk| {
                vdisk.replicas.iter().any(|replica| {
                    replica.node.name == self.local_node_name && replica.name == disk
                })
            })
            .map(|vdisk| vdisk.id.clone())
            .collect()
    }

    pub fn get_operation(&self, key: BobKey) -> BackendOperation {
        let vdisk_id = VDiskId::new((key.key % self.vdisks.len() as u64) as u32);
        let vdisk = self.vdisks.iter().find(|disk| disk.id == vdisk_id).unwrap();
        let disk = vdisk
            .replicas
            .iter()
            .find(|disk| disk.node.name == self.local_node_name);
        if disk.is_none() {
            trace!(
                "cannot find node: {} for vdisk: {}",
                self.local_node_name,
                vdisk_id
            );
            return BackendOperation::new_alien(vdisk_id);
        }
        BackendOperation::new_local(
            vdisk_id,
            DiskPath::new(&disk.unwrap().name, &disk.unwrap().path),
        )
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    // #[test]
    // pub fn test_nodes_link() {
    //     let nodes = vec![
    //         Node::new("1", "1", 1),
    //         Node::new("2", "1", 1),
    //         Node::new("3", "1", 1)];
    //     let tmp = VDiskMapper::link_nodes(nodes);

    //     assert_eq!(3, tmp.len());
    //     assert_eq!("2", tmp[0].get_next_node().name);        
    //     assert_eq!("3", tmp[1].get_next_node().name);
    //     assert_eq!("1", tmp[2].get_next_node().name);
    // }

    // #[test]
    // pub fn test_nodes_link_one_node() {
    //     let nodes = vec![Node::new("1", "1", 1)];
    //     let tmp = VDiskMapper::link_nodes(nodes);

    //     assert_eq!(1, tmp.len());
    //     assert_eq!("1", tmp[0].get_next_node().name);        
    // }

    // #[test]
    // pub fn test_nodes_link_chain() {
    //     let nodes = vec![
    //         Node::new("1", "1", 1),
    //         Node::new("2", "1", 1),
    //         Node::new("3", "1", 1)];
    //     let tmp = VDiskMapper::link_nodes(nodes);

    //     println!("aaaaaaaaaaaaa  {}", tmp[0]);
    //     assert_eq!(3, tmp.len());
    //     assert_eq!("3", tmp[0].get_next_node().get_next_node().name);
    // }
}