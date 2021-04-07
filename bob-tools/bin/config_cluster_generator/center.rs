use anyhow::{anyhow, Result as AnyResult};
use bob::{
    ClusterConfig, ClusterNodeConfig as ClusterNode, ClusterRackConfig as ClusterRack,
    ReplicaConfig as Replica, VDiskConfig as VDisk,
};
use std::{
    collections::{HashMap, HashSet},
    sync::atomic::{AtomicUsize, Ordering},
};

const ORD: Ordering = Ordering::Relaxed;

#[derive(Debug)]
pub struct Center {
    racks: Vec<Rack>,
}

impl Center {
    pub fn new() -> Self {
        Self { racks: Vec::new() }
    }

    pub fn mark_new(&mut self, disks: &[(&ClusterNode, &str)], racks: &[&ClusterRack]) {
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
        self.racks.push(item)
    }

    pub fn disks_count(&self) -> usize {
        self.racks.iter().fold(0, |acc, r| acc + r.disks_count())
    }

    fn next_disk(&self) -> AnyResult<(&Node, &Disk)> {
        let disks = self.racks.iter().flat_map(|r| {
            r.nodes
                .iter()
                .flat_map(|n| n.disks.iter().map(move |d| (n, d)))
        });
        let (node, disk) = disks
            .min_by_key(|r| r.1.used_count())
            .ok_or_else(|| anyhow!("Empty disks list"))?;
        node.inc();
        disk.inc();
        Ok((node, disk))
    }

    fn next_disk_from_list(&self, list: &[Replica]) -> AnyResult<(&Node, &Disk)> {
        let mut disks = self.racks.iter().flat_map(|r| {
            r.nodes
                .iter()
                .flat_map(|n| n.disks.iter().map(move |d| (n, d)))
        });

        let min_key = disks
            .clone()
            .map(|x| x.1.used_count())
            .min()
            .ok_or_else(|| anyhow!("Empty disks list"))?;

        let (node, disk) = disks
            .find(|(node, disk)| {
                disk.used_count() == min_key
                    && list.contains(&Replica::new(node.name.clone(), disk.name.clone()))
            })
            .ok_or_else(|| anyhow!("Can't find disk"))?;

        node.inc();
        disk.inc();
        Ok((node, disk))
    }

    fn next_rack(&self) -> AnyResult<&Rack> {
        let rack = self
            .racks
            .iter()
            .min_by_key(|r: &&Rack| r.used_count())
            .ok_or_else(|| anyhow!("Empty racks list"))?;
        rack.inc();
        Ok(rack)
    }

    fn next_rack_from_list(&self, list: &[&str]) -> AnyResult<&Rack> {
        if list.is_empty() {
            return Err(anyhow!("Empty list"));
        }
        let min_key = self
            .racks
            .iter()
            .map(|x| x.used_count())
            .min()
            .ok_or_else(|| anyhow!("Empty racks list"))?;

        let rack = self
            .racks
            .iter()
            .find(|&rack| rack.used_count() == min_key && list.contains(&rack.name.as_str()))
            .ok_or_else(|| anyhow!("Can't find rack"))?;

        rack.inc();
        Ok(rack)
    }

    pub fn create_vdisk(&self, id: u32, replicas_count: usize) -> VDisk {
        let mut vdisk = VDisk::new(id);
        let first_rack = self.next_rack().expect("no racks in setup");
        let (node, disk) = first_rack.next_disk(&[]).expect("no disks in setup");
        vdisk.push_replica(Replica::new(node.name.clone(), disk.name.clone()));

        while vdisk.replicas().len() < replicas_count {
            let banned_nodes = get_used_nodes_names(vdisk.replicas());
            let (node, disk) = if vdisk.replicas().len() == 2 {
                first_rack.next_disk(&banned_nodes).map_or_else(
                    |_| self.next_rack()?.next_disk(&banned_nodes),
                    |x| {
                        first_rack.inc();
                        Ok(x)
                    },
                )
            } else {
                self.next_rack()
                    .expect("no racks in setup")
                    .next_disk(&banned_nodes)
            }
            .expect("no disks in setup");
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
            .or_else(|_| self.next_disk())
            .expect("no disks in setup");
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
            .map(|rack| rack.name.as_str())
            .collect();

        while vdisk.replicas().len() < replicas_count {
            let rack = self
                .next_rack_from_list(&preferred_racks)
                .or_else(|_| self.next_rack())
                .expect("no racks in setup");
            let banned_nodes = get_used_nodes_names(vdisk.replicas());
            let (node, disk) = rack
                .next_disk_from_list(&banned_nodes, old_vdisk.replicas())
                .or_else(move |_| rack.next_disk(&banned_nodes))
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

    fn next_disk(&self, replicas: &[String]) -> AnyResult<(&Node, &Disk)> {
        let (node, disk) = self
            .nodes
            .iter()
            .filter(|node| !replicas.contains(&&node.name))
            .flat_map(|n| n.disks.iter().map(move |d| (n, d)))
            .min_by_key(|(_, d)| d.used_count())
            .unwrap_or(self.min_used_disk()?);
        node.inc();
        disk.inc();
        Ok((node, disk))
    }

    fn next_disk_from_list(
        &self,
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
            .map(|(_, d)| d.used_count())
            .min()
            .ok_or_else(|| anyhow!("Empty disks list"))?;

        let (node, disk) = disks
            .find(|(node, disk)| {
                min_key == disk.used_count()
                    && list.contains(&Replica::new(node.name.clone(), disk.name.clone()))
            })
            .ok_or_else(|| anyhow!("Can't find disk"))?;

        node.inc();
        disk.inc();
        Ok((node, disk))
    }

    fn min_used_disk(&self) -> AnyResult<(&Node, &Disk)> {
        self.nodes
            .iter()
            .flat_map(|n| n.disks.iter().map(move |d| (n, d)))
            .min_by_key(|(_, d)| d.used_count())
            .ok_or_else(|| anyhow!("Empty disks list"))
    }

    fn used_count(&self) -> usize {
        self.used_count.load(ORD)
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

    fn used_count(&self) -> usize {
        self.used_count.load(ORD)
    }
}

fn get_used_nodes_names(replicas: &[Replica]) -> Vec<String> {
    replicas.iter().map(|r| r.node().to_string()).collect()
}

pub fn get_pairs_count(nodes: &[ClusterNode]) -> usize {
    nodes.iter().fold(0, |acc, n| acc + n.disks().len())
}

pub fn get_structure(config: &ClusterConfig, use_racks: bool) -> AnyResult<Center> {
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
        debug!("get_structure: OK");
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
                    "config contains duplicate racks, get_structure: ERR [{}]",
                    rack.name()
                )
            })?;
        for node in rack.nodes() {
            node_rack_map.insert(node, rack.name())
            .ok_or_else(|| {
                anyhow!(
                            "config contains duplicate nodes in racks, get_structure: ERR [rack: {}, node: {}]",
                            rack.name(), node.as_str()
                )
            })?;
        }
    }
    for node in nodes {
        let rack_name = node_rack_map.get(&node.name).ok_or_else(|| {
            anyhow!(
                "not found rack for node, get_structure: ERR [node_name: {}]",
                &node.name
            )
        })?;
        let rack = racks.get_mut(rack_name).ok_or_else(|| {
            anyhow!(
                "rack not found, get_structure: ERR [rack_name: {}]",
                rack_name
            )
        })?;
        rack.nodes.push(node);
    }
    for (_, rack) in racks {
        center.push(rack);
    }
    debug!("get_structure: OK");
    Ok(center)
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
