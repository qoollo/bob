use crate::{
    configs::{
        cluster::{Cluster as ClusterConfig, DistributionFunc},
        node::Node as NodeConfig,
    },
    data::{BobKey, DiskPath, VDisk as DataVDisk, VDiskId},
    node::{Id as NodeId, Node},
};
use std::{
    collections::HashMap,
    convert::TryInto,
    sync::atomic::{AtomicUsize, Ordering},
};

/// Hash map with IDs as keys and `VDisk`s as values.
pub type VDisksMap = HashMap<VDiskId, DataVDisk>;

pub type NodesMap = HashMap<NodeId, Node>;

/// `get_support_nodes` helper
enum SupportIndexResult {
    Index(usize),
    Avail(usize),
}

/// Struct for managing distribution of replicas on disks and nodes.
/// Through the virtual intermediate object, called `VDisk` - "virtual disk"
#[derive(Debug)]
pub struct Virtual {
    local_node_name: String,
    local_node_address: String,
    disks: Vec<DiskPath>,
    vdisks: VDisksMap,
    nodes: Vec<Node>,
    distribution_func: DistributionFunc,
    support_nodes_offset: AtomicUsize,
}

impl Virtual {
    /// Creates new instance of the Virtual disk mapper
    pub async fn new(config: &NodeConfig, cluster: &ClusterConfig) -> Self {
        let mut vdisks = cluster.create_vdisks_map().unwrap();
        let nodes = Self::prepare_nodes(&mut vdisks, cluster).await;
        let local_node_name = config.name().to_owned();
        let local_node_address = nodes
            .iter()
            .find(|node| *node.name() == local_node_name)
            .expect("found node with name")
            .address()
            .to_string();
        let disks = config.disks();
        let disks_read = disks.lock().expect("mutex");
        Self {
            local_node_name,
            local_node_address,
            disks: disks_read.clone(),
            vdisks,
            nodes,
            distribution_func: cluster.distribution_func(),
            support_nodes_offset: AtomicUsize::new(0),
        }
    }

    async fn prepare_nodes(vdisks: &mut VDisksMap, cluster: &ClusterConfig) -> Vec<Node> {
        let nodes: Vec<Node> = cluster
            .nodes()
            .iter()
            .enumerate()
            .map(|(i, conf)| {
                let index = i.try_into().expect("usize to u16");
                let address = conf.address();
                let name = conf.name().to_owned();
                Node::new(name, address, index)
            })
            .collect();

        vdisks
            .values_mut()
            .for_each(|vdisk| vdisk.set_nodes(&nodes));
        nodes
    }

    pub fn local_node_name(&self) -> &str {
        &self.local_node_name
    }

    pub fn local_node_address(&self) -> &str {
        &self.local_node_address
    }

    pub fn vdisks_count(&self) -> u32 {
        self.vdisks.len().try_into().expect("usize to u32")
    }

    pub fn get_vdisks_ids(&self) -> Vec<VDiskId> {
        self.vdisks.keys().copied().collect()
    }

    pub fn local_disks(&self) -> &[DiskPath] {
        &self.disks
    }

    pub fn vdisks(&self) -> &VDisksMap {
        &self.vdisks
    }

    pub fn get_disk(&self, name: &str) -> Option<&DiskPath> {
        self.disks.iter().find(|d| d.name() == name)
    }

    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    pub fn distribution_func(&self) -> DistributionFunc {
        self.distribution_func
    }

    pub fn get_target_nodes_for_key(&self, key: BobKey) -> &[Node] {
        let id = self.vdisk_id_from_key(key);
        self.vdisks.get(&id).expect("vdisk not found").nodes()
    }

    fn find_support_node_index(&self, offset: usize) -> SupportIndexResult {
        let mut avail = 0;
        for i in 0..self.nodes.len() {
            let node = &self.nodes[i];
            if node.connection_available() {
                if avail == offset {
                    return SupportIndexResult::Index(i);
                }
                avail += 1;
            }
        }
        return SupportIndexResult::Avail(avail);
    }

    fn find_support_node_actual_offset(&self, offset: usize) -> usize {
        let mut res = self.find_support_node_index(offset % self.nodes.len());
        if let SupportIndexResult::Avail(avail) = res {
            if avail > 0 {
                let mut curr_offset = offset % avail;
                while let SupportIndexResult::Avail(avail) = res {
                    if avail == 0 {
                        break;
                    }
                    res = self.find_support_node_index(curr_offset);
                    curr_offset = if curr_offset > avail {
                        curr_offset - avail
                    } else {
                        0
                    }
                }
            }
        }
        match res {
            SupportIndexResult::Index(index) => index,
            SupportIndexResult::Avail(_) => offset % self.nodes.len(),
        }
    }

    pub fn get_support_nodes(&self, key: BobKey, count: usize) -> Vec<&Node> {
        debug_assert!(count <= self.nodes.len());
        if count == 0 {
            return vec![];
        }
        trace!("get target nodes for given key");
        let target_nodes = self.get_target_nodes_for_key(key);
        trace!("extract indexes of target nodes");
        trace!("nodes available: {}", self.nodes.len());
        let mut support_nodes: Vec<&Node> = Vec::with_capacity(count);

        let starting_index = self.find_support_node_actual_offset(
            self.support_nodes_offset.fetch_add(1, Ordering::Relaxed),
        );

        let len = self.nodes.len();
        for i in 0..len {
            let node = &self.nodes[(i + starting_index) % len];
            let conn = if i + starting_index < len {
                node.connection_available()
            } else {
                true
            };
            if conn
                && target_nodes.iter().all(|n| n.index() != node.index())
                && (i + starting_index < len
                    || support_nodes.iter().all(|&n| n.index() != node.index()))
            {
                support_nodes.push(node);
                if support_nodes.len() >= count {
                    break;
                }
            }
        }
        debug_assert!(support_nodes.len() <= count);
        support_nodes
    }

    pub fn vdisk_id_from_key(&self, key: BobKey) -> VDiskId {
        match self.distribution_func {
            DistributionFunc::Mod => (Self::get_vdisk_id_by_mod(key, self.vdisks.len()))
                .try_into()
                .expect("usize to u32"),
        }
    }

    fn get_vdisk_id_by_mod(key: BobKey, len: usize) -> usize {
        key.iter().fold([0, 1], |[rem, bmult], &byte| {
            [(rem + bmult * byte as usize) % len, (bmult << 8) % len]
        })[0]
    }

    /// Returns ref to `VDisk` with given ID
    #[must_use]
    pub fn get_vdisk(&self, vdisk_id: VDiskId) -> Option<&DataVDisk> {
        self.vdisks.get(&vdisk_id)
    }

    pub fn get_vdisk_for_key(&self, key: BobKey) -> Option<&DataVDisk> {
        let vdisk_id = self.vdisk_id_from_key(key);
        self.get_vdisk(vdisk_id)
    }

    pub fn get_vdisks_by_disk(&self, disk: &str) -> Vec<VDiskId> {
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

    pub fn get_operation(&self, key: BobKey) -> (VDiskId, Option<DiskPath>) {
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

    pub fn is_vdisk_on_node(&self, node_name: &str, id: VDiskId) -> bool {
        self.get_vdisk(id)
            .expect("vdisk not found")
            .nodes()
            .iter()
            .any(|node| node.name() == node_name)
    }
}
