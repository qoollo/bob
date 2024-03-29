use crate::{
    configs::{
        cluster::{Cluster as ClusterConfig, DistributionFunc},
        node::Node as NodeConfig,
    },
    data::BobKey,
    core_types::{DiskName, DiskPath, VDisk as DataVDisk, VDiskId},
    node::{NodeId, NodeName, Node},
};
use std::{
    collections::{HashMap, HashSet},
    convert::TryInto,
    sync::atomic::{AtomicUsize, Ordering},
};
use smallvec::SmallVec;

/// Hash map with IDs as keys and `VDisk`s as values.
pub type VDisksMap = HashMap<VDiskId, DataVDisk>;

/// `get_support_nodes` helper
struct SupportIndexResult {
    index: Option<usize>,
    avail: usize,
}

/// Struct for managing distribution of replicas on disks and nodes.
/// Through the virtual intermediate object, called `VDisk` - "virtual disk"
#[derive(Debug)]
pub struct Virtual {
    local_node_name: NodeName,
    local_node_address: String,
    disks: Vec<DiskPath>,
    vdisks: VDisksMap,
    nodes: Vec<Node>,
    distribution_func: DistributionFunc,
    support_nodes_offset: AtomicUsize,
}

impl Virtual {
    /// Creates new instance of the Virtual disk mapper
    pub fn new(config: &NodeConfig, cluster: &ClusterConfig) -> Self {
        let nodes = Self::prepare_nodes(cluster);
        let vdisks = Self::prepare_vdisks_map(cluster, nodes.as_slice());
        let local_node_name = config.name().into();
        let local_node_address = nodes
            .iter()
            .find(|node| *node.name() == local_node_name)
            .expect("found node with name")
            .address()
            .to_string();
        let disks = config.disks().lock().expect("mutex").clone();
        Self {
            local_node_name,
            local_node_address,
            disks,
            vdisks,
            nodes,
            distribution_func: cluster.distribution_func(),
            support_nodes_offset: AtomicUsize::new(0),
        }
    }

    fn prepare_nodes(cluster: &ClusterConfig) -> Vec<Node> {
        return cluster
            .nodes()
            .iter()
            .enumerate()
            .map(|(i, conf)| {
                let index = i.try_into().expect("usize to u16");
                Node::new(conf.name().into(), conf.address().to_owned(), index)
            })
            .collect();
    }
    fn prepare_vdisks_map(cluster: &ClusterConfig, nodes: &[Node]) -> VDisksMap {
        let mut vdisks = VDisksMap::new();
        let vdisks_replicas = cluster.collect_vdisk_replicas().unwrap();
        for (vdisk_id, cur_vdisk_replicas) in vdisks_replicas {
            // Collect nodes for vdisk
            let mut vdisk_nodes = Vec::new();
            for node in nodes {
                if cur_vdisk_replicas.iter().any(|r| r.node_name() == node.name()) {
                    vdisk_nodes.push(node.clone());
                }
            }

            vdisks.insert(vdisk_id, DataVDisk::new(vdisk_id, cur_vdisk_replicas, vdisk_nodes));
        }

        vdisks
    }

    pub fn local_node_name(&self) -> &NodeName {
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

    /// Skips nodes according to `offset` value and counts available nodes at the same time. 
    /// Available nodes: connected nodes and not in `target_nodes` list.
    /// If stops before the collection ends, then return value contains the node index for the specified offset.
    /// If it skips the whole collection of nodes, then return value contains the number of available nodes.
    fn try_find_support_node_offset_at_one_pass(
        nodes: &[Node],
        target_nodes: &[Node],
        offset: usize,
    ) -> SupportIndexResult {
        debug_assert!(offset <= nodes.len());

        let mut avail = 0;
        for i in 0..nodes.len() {
            let node = &nodes[i];
            if node.connection_available() && target_nodes.iter().all(|n| n.index() != node.index())
            {
                if avail == offset {
                    return SupportIndexResult {
                        index: Some(i),
                        avail: avail + 1,
                    };
                }
                avail += 1;
            }
        }
        return SupportIndexResult { index: None, avail };
    }

    /// Look for an offset respecting the uniform distribution for available nodes.
    /// Available nodes: connected nodes and not in `target_nodes` list.
    fn find_support_node_offset(nodes: &[Node], target_nodes: &[Node], offset: usize) -> usize {
        if target_nodes.len() >= nodes.len() {
            // target_nodes is equal to nodes => cannot find the proper offset
            return offset % nodes.len();
        }
        let mut res = Self::try_find_support_node_offset_at_one_pass(nodes, target_nodes, offset % (nodes.len() - target_nodes.len()));
        if res.index == None && res.avail > 0 {
            let mut curr_offset = offset % res.avail;
            while res.index == None && res.avail > 0 {
                res = Self::try_find_support_node_offset_at_one_pass(
                    nodes,
                    target_nodes,
                    curr_offset,
                );
                curr_offset = if curr_offset > res.avail {
                    curr_offset - res.avail
                } else {
                    0
                }
            }
        }
        match res.index {
            Some(index) => index,
            None => offset % nodes.len(),
        }
    }

    /// Returns vector of supported nodes (nodes that can be used to store aliens for specified key).
    /// Result vector will not conain target nodes for the key.
    /// Preserves uniform distribution along available nodes.
    pub fn get_support_nodes(&self, key: BobKey, count: usize) -> Vec<&Node> {
        debug_assert!(count <= self.nodes.len());
        if count == 0 {
            return vec![];
        }
        
        trace!("get target nodes for given key");
        let target_nodes = self.get_target_nodes_for_key(key);
        debug_assert!(target_nodes.iter().map(|n| n.index()).collect::<HashSet<NodeId>>().len() == target_nodes.len());
        debug_assert!(target_nodes.iter().all(|n| self.nodes.iter().any(|n_full| n.index() == n_full.index())));

        if target_nodes.len() >= self.nodes.len() {
            return vec![];
        }
        trace!("extract indexes of target nodes");
        trace!("nodes available: {}", self.nodes.len());
        let mut support_nodes: Vec<&Node> = Vec::with_capacity(count);

        // This offset search preserves the uniform distribution of aliens
        let starting_index = Self::find_support_node_offset(
            &self.nodes,
            target_nodes,
            self.support_nodes_offset.fetch_add(1, Ordering::Relaxed)
        );

        // First pass: fill supported nodes starting from `starting_index`
        let len = self.nodes.len();
        for i in 0..len {
            let node = &self.nodes[(i + starting_index) % len];
            if node.connection_available() && target_nodes.iter().all(|n| n.index() != node.index())
            {
                support_nodes.push(node);
                if support_nodes.len() >= count {
                    break;
                }
            }
        }

        if support_nodes.len() < count
            && support_nodes.len() + target_nodes.len() < self.nodes.len()
        {
            // Second pass: if the number of found support nodes is less than requested `count` then we also include diconnected ones
            for i in 0..len {
                let node = &self.nodes[(i + starting_index) % len];
                // Ignore connection status to fill support_nodes
                if support_nodes.iter().all(|n| n.index() != node.index())
                    && target_nodes.iter().all(|n| n.index() != node.index())
                {
                    support_nodes.push(node);
                    if support_nodes.len() >= count {
                        break;
                    }
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

    pub fn get_vdisks_by_disk(&self, disk: &DiskName) -> Vec<VDiskId> {
        let vdisks = self.vdisks.iter();
        vdisks
            .filter_map(|(id, vdisk)| {
                if vdisk
                    .replicas()
                    .iter()
                    .filter(|r| *r.node_name() == self.local_node_name)
                    .any(|replica| replica.disk_name() == disk)
                {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn get_operation(&self, key: BobKey) -> (VDiskId, Option<SmallVec<[DiskPath; 1]>>) {
        let virt_disk = self.get_vdisk_for_key(key).expect("vdisk not found");
        let mut disks = None;
        for replica in virt_disk.replicas() {
            if replica.node_name() == &self.local_node_name {
                if disks.is_none() {
                    disks = Some(SmallVec::<[DiskPath; 1]>::new());
                }
                disks.as_mut().expect("disks it not None here").push(DiskPath::from(replica));
            }
        }
        if disks.is_none() {
            debug!(
                "cannot find node: {} for vdisk: {}",
                self.local_node_name,
                virt_disk.id()
            );
        }

        (virt_disk.id(), disks)
    }

    pub fn is_vdisk_on_node(&self, node_name: &str, id: VDiskId) -> bool {
        self.get_vdisk(id)
            .expect("vdisk not found")
            .nodes()
            .iter()
            .any(|node| node.name() == node_name)
    }

    pub fn get_replicas_count_by_node(&self, key: BobKey) -> HashMap<NodeName, usize> {
        let mut result = HashMap::new();
        let id = self.vdisk_id_from_key(key);
        if let Some(vdisk) = self.vdisks.get(&id) {
            for replica in vdisk.replicas() {
                result.entry(replica.node_name().clone()).and_modify(|c| *c += 1).or_insert(1);
            }
        }
        result
    }
}
