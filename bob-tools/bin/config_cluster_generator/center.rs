use anyhow::{anyhow, Result as AnyResult};
use bob::{
    ClusterConfig, ClusterNodeConfig as ClusterNode, ClusterRackConfig as ClusterRack, DiskPath,
    ReplicaConfig as Replica, VDiskConfig as VDisk,
};
use std::{
    collections::{HashMap, HashSet},
    ops::AddAssign,
    sync::atomic::{AtomicUsize, Ordering},
};

const ORD: Ordering = Ordering::Relaxed;
const REPLICA_IN_FIRST_RACK: usize = 2;

#[derive(Debug, Default)]
struct Counter {
    racks_used_count: HashMap<String, usize>,
    nodes_used_count: HashMap<String, usize>,
    disks_used_count: HashMap<(String, String), usize>,
}

impl Counter {
    fn add_rack(&mut self, rack: &Rack) {
        self.racks_used_count.insert(rack.name.clone(), 0);
    }
    fn add_node(&mut self, node: &Node) {
        self.nodes_used_count.insert(node.name.clone(), 0);
    }
    fn add_disk(&mut self, node: &Node, disk: &Disk) {
        self.disks_used_count
            .insert((node.name.clone(), disk.name.clone()), 0);
    }
    fn inc_rack_used_count(&mut self, rack: &Rack) {
        self.racks_used_count
            .get_mut(&rack.name)
            .expect("added by constructor")
            .add_assign(1);
    }

    fn inc_node_used_count(&mut self, node: &Node) {
        self.nodes_used_count
            .get_mut(&node.name)
            .expect("added by constructor")
            .add_assign(1);
    }

    fn inc_disk_used_count(&mut self, node: &Node, disk: &Disk) {
        self.disks_used_count
            .get_mut(&(node.name.clone(), disk.name.clone()))
            .expect("added by constructor")
            .add_assign(1);
    }

    fn rack_used_count(&self, rack: &Rack) -> usize {
        *self
            .racks_used_count
            .get(&rack.name)
            .expect("added by constructor")
    }

    fn node_used_count(&self, node: &Node) -> usize {
        *self
            .nodes_used_count
            .get(&node.name)
            .expect("added by constructor")
    }

    fn disk_used_count(&self, node: &Node, disk: &Disk) -> usize {
        *self
            .disks_used_count
            .get(&(node.name.clone(), disk.name.clone()))
            .expect("added by constructor")
    }
}

#[derive(Debug, Default)]
pub struct Center {
    racks: Vec<Rack>,
    counter: Counter,
}

impl Center {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_cluster_config(config: &ClusterConfig, use_racks: bool) -> AnyResult<Center> {
        let nodes = config
            .nodes()
            .iter()
            .map(|node| {
                Node::new(
                    node.name(),
                    node.disks()
                        .iter()
                        .map(|d| Disk::new(d.name(), true))
                        .collect(),
                    true,
                )
            })
            .collect();
        let mut center = Center::new();
        if !use_racks || config.racks().is_empty() {
            let rack = Rack::new("unknown".to_string(), nodes, true);
            center.push(rack);
            debug!("Center::from_cluster_config: OK");
            return Ok(center);
        }
        let mut racks = HashMap::new();
        let mut node_rack_map = HashMap::new();
        for rack in config.racks() {
            racks
                .insert(
                    rack.name(),
                    Rack::new(rack.name().to_string(), vec![], true),
                )
                .ok_or_else(|| {
                    anyhow!(
                        "Center::from_cluster_config: config contains duplicate racks [{}]",
                        rack.name()
                    )
                })?;
            for node in rack.nodes() {
                node_rack_map.insert(node, rack.name())
            .ok_or_else(|| {
                anyhow!(
                            "Center::from_cluster_config: config contains duplicate nodes in racks [rack: {}, node: {}]",
                            rack.name(), node.as_str()
                )
            })?;
            }
        }
        for node in nodes {
            let rack_name = node_rack_map.get(&node.name).ok_or_else(|| {
                anyhow!(
                    "Center::from_cluster_config: not found rack for node [node_name: {}]",
                    &node.name
                )
            })?;
            let rack = racks.get_mut(rack_name).ok_or_else(|| {
                anyhow!(
                    "Center::from_cluster_config: rack not found [rack_name: {}]",
                    rack_name
                )
            })?;
            rack.nodes.push(node);
        }
        for (_, rack) in racks {
            center.push(rack);
        }
        if center.disks_count() == 0 {
            return Err(anyhow!(
                "Center::from_cluster_config: config should contain disks"
            ));
        }
        debug!("Center::from_cluster_config: OK");
        Ok(center)
    }

    pub fn mark_new(&mut self, disks: &[(&ClusterNode, &DiskPath)], racks: &[&ClusterRack]) {
        for rack in self.racks.iter_mut() {
            if racks.iter().any(|&new_rack| rack.name == new_rack.name()) {
                rack.is_old = false;
            }
            rack.mark_new(disks);
        }
        self.racks.sort_unstable_by_key(|a| match a.is_old {
            true => 0,
            false => 1,
        });
    }

    pub fn push(&mut self, item: Rack) {
        self.counter.add_rack(&item);
        for node in item.nodes.iter() {
            self.counter.add_node(node);
            for disk in node.disks.iter() {
                self.counter.add_disk(node, disk);
            }
        }
        self.racks.push(item)
    }

    pub fn disks_count(&self) -> usize {
        self.racks.iter().fold(0, |acc, r| acc + r.disks_count())
    }

    fn next_disk(&mut self) -> (&Node, &Disk) {
        let disks = self.racks.iter().flat_map(|r| {
            r.nodes
                .iter()
                .flat_map(|n| n.disks.iter().map(move |d| (n, d)))
        });
        let (node, disk) = disks
            .min_by_key(|(node, disk)| self.counter.disk_used_count(node, disk))
            .expect("checked by constructor");
        self.counter.inc_node_used_count(node);
        self.counter.inc_disk_used_count(node, disk);
        (node, disk)
    }

    fn next_disk_from_list(&mut self, list: &[Replica]) -> AnyResult<(&Node, &Disk)> {
        let mut disks = self.racks.iter().flat_map(|r| {
            r.nodes
                .iter()
                .flat_map(|n| n.disks.iter().map(move |d| (n, d)))
        });

        let min_key = disks
            .clone()
            .map(|(node, disk)| self.counter.disk_used_count(node, disk))
            .min()
            .expect("checked by constructor");

        let (node, disk) = disks
            .find(|(node, disk)| {
                self.counter.disk_used_count(node, disk) == min_key
                    && list.contains(&Replica::new(node.name.clone(), disk.name.clone()))
            })
            .ok_or_else(|| anyhow!("Can't find disk"))?;

        self.counter.inc_node_used_count(node);
        self.counter.inc_disk_used_count(node, disk);
        Ok((node, disk))
    }

    fn next_rack(&mut self) -> &Rack {
        let rack = self
            .racks
            .iter()
            .min_by_key(|rack| self.counter.rack_used_count(rack))
            .expect("checked by constructor");
        self.counter.inc_rack_used_count(rack);
        rack
    }

    fn next_rack_from_list(&mut self, list: &[String]) -> AnyResult<&Rack> {
        if list.is_empty() {
            return Err(anyhow!("Empty list"));
        }
        let min_key = self
            .racks
            .iter()
            .map(|rack| self.counter.rack_used_count(rack))
            .min()
            .expect("checked by constructor");

        let rack = self
            .racks
            .iter()
            .find(|&rack| {
                self.counter.rack_used_count(rack) == min_key && list.contains(&rack.name)
            })
            .ok_or_else(|| anyhow!("Can't find rack"))?;

        self.counter.inc_rack_used_count(rack);
        Ok(rack)
    }

    fn should_place_in_first_rack(vdisk: &VDisk) -> bool {
        vdisk.replicas().len() == REPLICA_IN_FIRST_RACK
    }

    pub fn create_vdisk(&mut self, id: u32, replicas_count: usize) -> VDisk {
        let mut vdisk = VDisk::new(id);
        let first_rack = self.next_rack();
        let (node, disk) = first_rack
            .next_disk(&mut self.counter, &[])
            .expect("no disks in setup");
        vdisk.push_replica(Replica::new(node.name.clone(), disk.name.clone()));

        while vdisk.replicas().len() < replicas_count {
            let banned_nodes = get_used_nodes_names(vdisk.replicas());
            let (node, disk) = if Center::should_place_in_first_rack(&vdisk) {
                match first_rack.next_disk(&mut self.counter, &banned_nodes) {
                    Ok(x) => {
                        self.counter.inc_rack_used_count(first_rack);
                        Ok(x)
                    }
                    Err(_) => self.next_rack().next_disk(&mut self.counter, &banned_nodes),
                }
            } else {
                self.next_rack().next_disk(&mut self.counter, &banned_nodes)
            }
            .expect("no disks in setup");
            vdisk.push_replica(Replica::new(node.name.clone(), disk.name.clone()));
            debug!("replica added: {} {}", node.name, disk.name);
        }
        vdisk
    }

    pub fn create_vdisk_from_another(&mut self, old_vdisk: &VDisk) -> VDisk {
        let replicas_count = old_vdisk.replicas().len();
        let mut vdisk = VDisk::new(old_vdisk.id());
        let (node, disk) = match self.next_disk_from_list(old_vdisk.replicas()) {
            Ok(x) => x,
            Err(_) => self.next_disk(),
        };
        vdisk.push_replica(Replica::new(node.name.clone(), disk.name.clone()));

        let preferred_racks: Vec<_> = self
            .racks
            .iter()
            .filter(|&rack| {
                let node_names: Vec<_> = rack.nodes.iter().map(|node| node.name.as_str()).collect();
                old_vdisk
                    .replicas()
                    .iter()
                    .map(|r| r.node())
                    .any(|node| node_names.contains(&node))
            })
            .map(|rack| rack.name.as_str().to_owned())
            .collect();

        while vdisk.replicas().len() < replicas_count {
            let banned_nodes = get_used_nodes_names(vdisk.replicas());
            let rack = match self.next_rack_from_list(&preferred_racks) {
                Ok(x) => x,
                Err(_) => self.next_rack(),
            };
            let (node, disk) = match rack.next_disk_from_list(
                &mut self.counter,
                &banned_nodes,
                old_vdisk.replicas(),
            ) {
                Ok(x) => Ok(x),
                Err(_) => rack.next_disk(&mut self.counter, &banned_nodes),
            }
            .expect("no disks in setup");
            vdisk.push_replica(Replica::new(node.name.clone(), disk.name.clone()));
            debug!("replica added: {} {}", node.name, disk.name);
        }
        vdisk
    }

    pub fn validate(&self) -> AnyResult<()> {
        let mut racks_names: Vec<_> = self.racks.iter().map(|r| r.name.as_str()).collect();
        racks_names.sort_unstable();
        if racks_names.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(anyhow!("config contains duplicates racks names"));
        }

        let mut nodes_names: Vec<_> = self
            .racks
            .iter()
            .flat_map(|r| r.nodes.iter().map(|n| n.name.as_str()))
            .collect();
        nodes_names.sort_unstable();
        if nodes_names.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(anyhow!("config contains duplicates nodes names"));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct Rack {
    name: String,
    used_count: AtomicUsize,
    nodes: Vec<Node>,
    is_old: bool,
}

impl Rack {
    pub fn new(name: String, nodes: Vec<Node>, is_old: bool) -> Rack {
        Rack {
            name,
            used_count: AtomicUsize::new(0),
            nodes,
            is_old,
        }
    }

    pub fn mark_new(&mut self, disks: &[(&ClusterNode, &DiskPath)]) {
        for node in self.nodes.iter_mut() {
            node.mark_new(disks);
        }
        self.nodes.sort_unstable_by_key(|a| match a.is_old {
            true => 0,
            false => 1,
        });
    }

    fn disks_count(&self) -> usize {
        self.nodes.iter().fold(0, |acc, n| acc + n.disks_count())
    }

    fn next_disk(&self, counter: &mut Counter, replicas: &[String]) -> AnyResult<(&Node, &Disk)> {
        let (node, disk) = self
            .nodes
            .iter()
            .filter(|node| !replicas.contains(&&node.name))
            .flat_map(|n| n.disks.iter().map(move |d| (n, d)))
            .min_by_key(|(node, disk)| counter.disk_used_count(node, disk))
            .unwrap_or(self.min_used_disk(counter)?);
        counter.inc_node_used_count(node);
        counter.inc_disk_used_count(node, disk);
        Ok((node, disk))
    }

    fn next_disk_from_list(
        &self,
        counter: &mut Counter,
        replicas: &[String],
        list: &[Replica],
    ) -> AnyResult<(&Node, &Disk)> {
        let mut disks = self
            .nodes
            .iter()
            .filter(|node| !replicas.contains(&&node.name))
            .flat_map(|n| n.disks.iter().map(move |d| (n, d)));

        let min_key = disks
            .clone()
            .map(|(node, disk)| counter.disk_used_count(node, disk))
            .min()
            .ok_or_else(|| anyhow!("Empty disks list"))?;

        let (node, disk) = disks
            .find(|(node, disk)| {
                min_key == counter.disk_used_count(node, disk)
                    && list.contains(&Replica::new(node.name.clone(), disk.name.clone()))
            })
            .ok_or_else(|| anyhow!("Can't find disk"))?;

        counter.inc_node_used_count(node);
        counter.inc_disk_used_count(node, disk);
        Ok((node, disk))
    }

    fn min_used_disk(&self, counter: &Counter) -> AnyResult<(&Node, &Disk)> {
        self.nodes
            .iter()
            .flat_map(|n| n.disks.iter().map(move |d| (n, d)))
            .min_by_key(|(node, disk)| counter.disk_used_count(node, disk))
            .ok_or_else(|| anyhow!("Empty disks list"))
    }
}

#[derive(Debug)]
pub struct Node {
    name: String,
    used_count: AtomicUsize,
    disks: Vec<Disk>,
    is_old: bool,
}

impl Node {
    pub fn new(name: impl Into<String>, disks: Vec<Disk>, is_old: bool) -> Self {
        Self {
            name: name.into(),
            used_count: AtomicUsize::new(0),
            disks,
            is_old,
        }
    }

    pub fn mark_new(&mut self, disks: &[(&ClusterNode, &DiskPath)]) {
        let mut old_node = false;
        for disk in self.disks.iter_mut() {
            for (new_node, new_disk) in disks {
                if self.name == new_node.name() && disk.name.as_str() == new_disk.name() {
                    disk.is_old = false;
                    break;
                } else {
                    old_node = true;
                }
            }
        }
        if !old_node {
            self.is_old = false;
        }
        self.disks.sort_unstable_by_key(|a| match a.is_old {
            true => 0,
            false => 1,
        });
    }

    fn disks_count(&self) -> usize {
        self.disks.len()
    }
}

#[derive(Debug)]
pub struct Disk {
    name: String,
    used_count: AtomicUsize,
    is_old: bool,
}

impl Disk {
    pub fn new(name: impl Into<String>, is_old: bool) -> Self {
        Self {
            name: name.into(),
            used_count: AtomicUsize::new(0),
            is_old,
        }
    }
}

fn get_used_nodes_names(replicas: &[Replica]) -> Vec<String> {
    replicas.iter().map(|r| r.node().to_string()).collect()
}

pub fn get_pairs_count(nodes: &[ClusterNode]) -> usize {
    nodes.iter().fold(0, |acc, n| acc + n.disks().len())
}

pub fn get_new_disks<'a>(
    old_nodes: &'a [ClusterNode],
    new_nodes: &'a [ClusterNode],
) -> impl Iterator<Item = (&'a ClusterNode, &'a DiskPath)> {
    let old_disks = old_nodes
        .iter()
        .flat_map(|node| node.disks().iter().map(move |disk| (node, disk)));
    new_nodes
        .iter()
        .flat_map(|node| node.disks().iter().map(move |disk| (node, disk)))
        .filter(move |(node, disk)| {
            old_disks
                .clone()
                .find(|(old_node, old_disk)| {
                    node.name() == old_node.name()
                        && node.address() == old_node.address()
                        && disk == old_disk
                })
                .is_none()
        })
}

pub fn get_new_racks<'a>(
    old_racks: &'a [ClusterRack],
    new_racks: &'a [ClusterRack],
) -> impl Iterator<Item = &'a ClusterRack> {
    new_racks.iter().filter(move |&rack| {
        !old_racks
            .iter()
            .any(move |old_rack| rack.name() == old_rack.name())
    })
}

pub fn check_expand_configs(old: &Center, new: &Center, use_racks: bool) -> AnyResult<()> {
    if use_racks {
        let old_rack_names: HashSet<_> = old.racks.iter().map(|r| r.name.as_str()).collect();
        let new_rack_names: HashSet<_> = new.racks.iter().map(|r| r.name.as_str()).collect();
        if !old_rack_names.is_subset(&new_rack_names) {
            return Err(anyhow!(
                concat!(
                    "some racks was removed from config, ",
                    "check_expand_configs: ERR [{:?}]"
                ),
                old_rack_names.difference(&new_rack_names)
            ));
        }
    }
    let old_node_disk_pairs: HashSet<_> = old
        .racks
        .iter()
        .flat_map(|r| r.nodes.iter())
        .flat_map(|n| {
            n.disks
                .iter()
                .map(move |d| (n.name.as_str(), d.name.as_str()))
        })
        .collect();
    let new_node_disk_pairs: HashSet<_> = new
        .racks
        .iter()
        .flat_map(|r| r.nodes.iter())
        .flat_map(|n| {
            n.disks
                .iter()
                .map(move |d| (n.name.as_str(), d.name.as_str()))
        })
        .collect();
    if !old_node_disk_pairs.is_subset(&new_node_disk_pairs) {
        return Err(anyhow!(
            concat!(
                "some nodes or disks was removed from config, ",
                "check_expand_configs: ERR [{:?}]"
            ),
            old_node_disk_pairs.difference(&new_node_disk_pairs)
        ));
    }
    Ok(())
}
