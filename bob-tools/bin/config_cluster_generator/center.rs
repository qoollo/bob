use bob::{
    ClusterConfig, ClusterNodeConfig as ClusterNode, ReplicaConfig as Replica, VDiskConfig as VDisk,
};
use std::sync::atomic::{AtomicUsize, Ordering};

const ORD: Ordering = Ordering::Relaxed;

#[derive(Debug)]
pub struct Center {
    racks: Vec<Rack>,
}

impl Center {
    pub fn new() -> Self {
        Self { racks: Vec::new() }
    }

    pub fn mark_new(&mut self, disks: &[(&ClusterNode, &str)]) {
        for rack in self.racks.iter_mut() {
            rack.mark_new(disks);
        }
        self.racks.sort_unstable_by_key(|a| match a.is_old {
            true => 0,
            false => 1,
        });
    }

    pub fn push(&mut self, item: Rack) {
        self.racks.push(item)
    }

    pub fn disks_count(&self) -> usize {
        self.racks.iter().fold(0, |acc, r| acc + r.disks_count())
    }

    fn next_disk(&self) -> Option<(&Node, &Disk)> {
        let disks = self.racks.iter().flat_map(|r| {
            r.nodes
                .iter()
                .flat_map(|n| n.disks.iter().map(move |d| (n, d)))
        });
        let (node, disk) = disks.min_by_key(|r| r.1.used_count.load(ORD))?;
        node.inc();
        disk.inc();
        Some((node, disk))
    }

    fn next_disk_from_list(&self, list: &[Replica]) -> Option<(&Node, &Disk)> {
        let mut disks = self.racks.iter().flat_map(|r| {
            r.nodes
                .iter()
                .flat_map(|n| n.disks.iter().map(move |d| (n, d)))
        });

        let min_key = disks.clone().map(|x| x.1.used_count.load(ORD)).min()?;

        let (node, disk) = disks.find(|(node, disk)| {
            disk.used_count.load(ORD) == min_key
                && list.contains(&Replica::new(node.name.clone(), disk.name.clone()))
        })?;

        node.inc();
        disk.inc();
        Some((node, disk))
    }

    fn next_rack(&self) -> Option<&Rack> {
        let rack = self
            .racks
            .iter()
            .min_by_key(|r: &&Rack| r.used_count.load(ORD))?;
        rack.inc();
        Some(rack)
    }

    pub fn create_vdisk(&self, id: u32, replicas_count: usize) -> VDisk {
        let mut vdisk = VDisk::new(id);
        let (node, disk) = self.next_disk().expect("no disks in setup");
        vdisk.push_replica(Replica::new(node.name.clone(), disk.name.clone()));

        while vdisk.replicas().len() < replicas_count {
            let rack = self.next_rack().expect("no racks in setup");
            let banned_nodes = get_used_nodes_names(vdisk.replicas());
            let (node, disk) = rack.next_disk(&banned_nodes).expect("no disks in setup");
            vdisk.push_replica(Replica::new(node.name.clone(), disk.name.clone()));
            debug!("replica added: {} {}", node.name, disk.name);
        }
        vdisk
    }

    pub fn create_vdisk_from_another(&self, old_vdisk: &VDisk) -> VDisk {
        let replicas_count = old_vdisk.replicas().len();
        let mut vdisk = VDisk::new(old_vdisk.id());
        let (node, disk) = self
            .next_disk_from_list(old_vdisk.replicas())
            .or_else(move || self.next_disk())
            .expect("no disks in setup");
        vdisk.push_replica(Replica::new(node.name.clone(), disk.name.clone()));

        while vdisk.replicas().len() < replicas_count {
            let rack = self.next_rack().expect("no racks in setup");
            let banned_nodes = get_used_nodes_names(vdisk.replicas());
            let (node, disk) = rack
                .next_disk_from_list(&banned_nodes, old_vdisk.replicas())
                .or_else(move || rack.next_disk(&banned_nodes))
                .expect("no disks in setup");
            vdisk.push_replica(Replica::new(node.name.clone(), disk.name.clone()));
            debug!("replica added: {} {}", node.name, disk.name);
        }
        vdisk
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

    pub fn mark_new(&mut self, disks: &[(&ClusterNode, &str)]) {
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

    fn inc(&self) {
        self.used_count.fetch_add(1, ORD);
    }

    fn next_disk(&self, replicas: &[String]) -> Option<(&Node, &Disk)> {
        let (node, disk) = self
            .nodes
            .iter()
            .filter(|node| !replicas.contains(&&node.name))
            .flat_map(|n| n.disks.iter().map(move |d| (n, d)))
            .min_by_key(|(_, d)| d.used_count.load(ORD))
            .unwrap_or(self.min_used_disk()?);
        node.inc();
        disk.inc();
        Some((node, disk))
    }

    fn next_disk_from_list(&self, replicas: &[String], list: &[Replica]) -> Option<(&Node, &Disk)> {
        let mut disks = self
            .nodes
            .iter()
            .filter(|node| !replicas.contains(&&node.name))
            .flat_map(|n| n.disks.iter().map(move |d| (n, d)));

        let min_key = disks.clone().map(|(_, d)| d.used_count.load(ORD)).min()?;

        let (node, disk) = disks.find(|(node, disk)| {
            min_key == disk.used_count.load(ORD)
                && list.contains(&Replica::new(node.name.clone(), disk.name.clone()))
        })?;

        node.inc();
        disk.inc();
        Some((node, disk))
    }

    fn min_used_disk(&self) -> Option<(&Node, &Disk)> {
        self.nodes
            .iter()
            .flat_map(|n| n.disks.iter().map(move |d| (n, d)))
            .min_by_key(|(_, d)| d.used_count.load(ORD))
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

    pub fn mark_new(&mut self, disks: &[(&ClusterNode, &str)]) {
        let mut old_node = false;
        for disk in self.disks.iter_mut() {
            for (new_node, new_disk) in disks {
                if self.name == new_node.name() && disk.name.as_str() == *new_disk {
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

    fn inc(&self) {
        self.used_count.fetch_add(1, ORD);
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

    fn inc(&self) {
        self.used_count.fetch_add(1, ORD);
    }
}

fn get_used_nodes_names(replicas: &[Replica]) -> Vec<String> {
    replicas.iter().map(|r| r.node().to_string()).collect()
}

pub fn get_pairs_count(nodes: &[ClusterNode]) -> usize {
    nodes.iter().fold(0, |acc, n| acc + n.disks().len())
}

pub fn get_structure(config: &ClusterConfig) -> Center {
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
    let rack = Rack::new("unknown".to_string(), nodes, true);
    center.push(rack);
    center
}

pub fn get_new_disks<'a>(
    old_nodes: &'a [ClusterNode],
    new_nodes: &'a [ClusterNode],
) -> impl Iterator<Item = (&'a ClusterNode, &'a str)> {
    let old_disks = old_nodes
        .iter()
        .flat_map(|node| node.disks().iter().map(move |disk| (node, disk.name())));
    new_nodes
        .iter()
        .flat_map(|node| node.disks().iter().map(move |disk| (node, disk.name())))
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
