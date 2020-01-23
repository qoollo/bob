#[macro_use]
extern crate log;

use bob::configs::cluster::{Config, Node as ClusterNode, Replica, VDisk};
use chrono::Local;
use clap::{App, Arg, ArgMatches};
use env_logger::fmt::Color;
use log::{Level, LevelFilter};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};

const ORD: Ordering = Ordering::Relaxed;

#[tokio::main]
async fn main() {
    init_logger();
    if let Some(output) = read_from_file().and_then(generate_config) {
        write_to_file(output);
        debug!("config cluster generation: OK");
    } else {
        debug!("config cluster generation: ERR");
    }
}

fn init_logger() {
    env_logger::builder()
        .format(|buf, record: &log::Record| {
            let mut style = buf.style();
            let color = match record.level() {
                Level::Error => Color::Red,
                Level::Warn => Color::Yellow,
                Level::Info => Color::Green,
                Level::Debug => Color::Cyan,
                Level::Trace => Color::White,
            };
            style.set_color(color);
            writeln!(
                buf,
                "[{} {}:{:^4} {:^5}] - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.module_path().unwrap_or(""),
                record.line().unwrap_or(0),
                style.value(record.level()),
                record.args(),
            )
        })
        .filter_level(LevelFilter::Trace)
        .try_init()
        .expect("other logger already started");
    debug!("init logger: OK");
}

fn read_from_file() -> Option<Config> {
    let name = get_name();
    let file = open_file(name)?;
    let content = read_file(file)?;
    let config = deserialize(content)?;
    debug!("read from file: OK");
    Some(config)
}

fn get_name() -> String {
    let matches = get_matches();
    debug!("get matches: OK");
    let name = matches
        .value_of("input")
        .expect("is some, because of default arg value");
    debug!("get name: OK [{}]", name);
    name.to_owned()
}

fn open_file(name: String) -> Option<File> {
    OpenOptions::new()
        .read(true)
        .create(false)
        .open(name)
        .map(|f| {
            debug!("open file: OK");
            f
        })
        .map_err(|e| error!("open file: ERR [{}]", e))
        .ok()
}

fn read_file(mut file: File) -> Option<String> {
    let mut buf = String::new();
    file.read_to_string(&mut buf)
        .map(|n| debug!("read file: OK [{}b]", n))
        .map_err(|e| error!("read file: ERR [{}]", e))
        .ok()?;
    Some(buf)
}

fn deserialize(content: String) -> Option<Config> {
    serde_yaml::from_str(content.as_str())
        .map(|c: Config| {
            debug!("deserialize: OK [nodes count: {}]", c.nodes.len());
            c
        })
        .map_err(|e| error!("deserialize: ERR [{}]", e))
        .ok()
}

fn generate_config(input: Config) -> Option<Config> {
    let replicas_count = get_replicas_count()?;
    let vdisks_count = get_vdisks_count(&input.nodes)?;
    let res = simple_gen(input, replicas_count, vdisks_count);
    debug!("generate config: OK");
    Some(res)
}

fn replica_from_pair(node: &Node, disk: &Disk) -> Replica {
    Replica {
        node: Some(node.name.clone()),
        disk: Some(disk.name.clone()),
    }
}

fn get_used_nodes_names(replicas: &[Replica]) -> Vec<String> {
    replicas.iter().map(|r| r.node().to_string()).collect()
}

fn new_vdisk(id: i32) -> VDisk {
    VDisk {
        id: Some(id),
        replicas: Vec::new(),
    }
}

#[derive(Debug)]
struct Center {
    racks: Vec<Rack>,
}

impl Center {
    fn new() -> Self {
        Self { racks: Vec::new() }
    }

    fn push(&mut self, item: Rack) {
        self.racks.push(item)
    }

    fn disks_count(&self) -> usize {
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

    fn next_rack(&self) -> Option<&Rack> {
        let rack = self
            .racks
            .iter()
            .min_by_key(|r: &&Rack| r.used_count.load(ORD))?;
        rack.inc();
        Some(rack)
    }

    fn create_vdisk(&self, id: i32, replicas_count: usize) -> VDisk {
        let mut vdisk = new_vdisk(id);
        let (node, disk) = self.next_disk().expect("no disks in setup");
        vdisk.replicas.push(replica_from_pair(node, disk));

        while vdisk.replicas.len() < replicas_count {
            let rack = self.next_rack().expect("no racks in setup");
            let banned_nodes = get_used_nodes_names(&vdisk.replicas);
            let (node, disk) = rack.next_disk(&banned_nodes).expect("no disks in setup");
            vdisk.replicas.push(replica_from_pair(node, disk));
            debug!("replica added: {} {}", node.name, disk.name);
        }
        vdisk
    }
}

#[derive(Debug)]
struct Rack {
    name: String,
    used_count: AtomicUsize,
    nodes: Vec<Node>,
}

impl Rack {
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

    fn min_used_disk(&self) -> Option<(&Node, &Disk)> {
        self.nodes
            .iter()
            .flat_map(|n| n.disks.iter().map(move |d| (n, d)))
            .min_by_key(|(_, d)| d.used_count.load(ORD))
    }
}

#[derive(Debug)]
struct Node {
    name: String,
    used_count: AtomicUsize,
    disks: Vec<Disk>,
}

impl Node {
    fn new(name: impl Into<String>, disks: Vec<Disk>) -> Self {
        Self {
            name: name.into(),
            used_count: AtomicUsize::new(0),
            disks,
        }
    }

    fn disks_count(&self) -> usize {
        self.disks.len()
    }

    fn inc(&self) {
        self.used_count.fetch_add(1, ORD);
    }
}

#[derive(Debug)]
struct Disk {
    name: String,
    used_count: AtomicUsize,
}

impl Disk {
    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            used_count: AtomicUsize::new(0),
        }
    }

    fn inc(&self) {
        self.used_count.fetch_add(1, ORD);
    }
}

/// greatest common divider
fn gcd(a: usize, b: usize) -> usize {
    debug!("gcd of {} and {}", a, b);
    if a == 0 {
        b
    } else if b == 0 {
        a
    } else {
        gcd(b, a % b)
    }
}

/// least common multiple
fn lcm(a: usize, b: usize) -> usize {
    let lcm = a / gcd(a, b) * b;
    debug!("lcm of {} and {} is {}", a, b, lcm);
    lcm
}

fn simple_gen(mut config: Config, replicas_count: usize, vdisks_count: usize) -> Config {
    let center = get_structure(&config);
    let vdisks_count = vdisks_count.max(lcm(center.disks_count(), replicas_count));
    debug!("new vdisks count: OK [{}]", vdisks_count);
    let mut vdisks = Vec::new();
    while vdisks.len() < vdisks_count {
        let vdisk = center.create_vdisk(vdisks.len() as i32, replicas_count);
        debug!("vdisk added: {}", vdisk.id());
        vdisks.push(vdisk);
    }
    config.vdisks = vdisks;
    debug!("simple gen: OK [\n{:#?}\n]", center);
    config
}

fn get_structure(config: &Config) -> Center {
    let nodes = config
        .nodes
        .iter()
        .map(|node| {
            Node::new(
                node.name(),
                node.disks.iter().map(|d| Disk::new(d.name())).collect(),
            )
        })
        .collect();
    let mut center = Center::new();
    let rack = Rack {
        name: "unknown".to_string(),
        used_count: AtomicUsize::new(0),
        nodes,
    };
    center.push(rack);
    center
}

fn get_replicas_count() -> Option<usize> {
    let matches = get_matches();
    matches
        .value_of("replicas")
        .expect("replicas count")
        .parse()
        .map_err(|e| error!("get replicas count: ERR [{}]", e))
        .ok()
}

fn get_vdisks_count(nodes: &[ClusterNode]) -> Option<usize> {
    let matches = get_matches();
    matches.value_of("vdisks_count").map_or_else(
        || {
            let res = get_pairs_count(nodes);
            debug!("get vdisks count: OK [{}]", res);
            Some(res)
        },
        |s| {
            s.parse()
                .map_err(|e| error!("generate config: ERR [{}]", e))
                .ok()
        },
    )
}

fn get_pairs_count(nodes: &[ClusterNode]) -> usize {
    nodes.iter().fold(0, |acc, n| acc + n.disks.len())
}

fn write_to_file(output: Config) {
    let name = get_matches()
        .value_of("output")
        .expect("is some, default value is set")
        .to_owned();
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(name)
        .expect("File IO error");
    let mut output = serde_yaml::to_string(&output).expect("config serialization error");
    output += "\n";
    file.write_all(output.as_bytes()).expect("File IO error");
    debug!("write to file: OK");
}

fn get_matches() -> ArgMatches<'static> {
    let input = Arg::with_name("input")
        .short("i")
        .default_value("cluster.yaml")
        .takes_value(true);
    let output = Arg::with_name("output")
        .short("o")
        .default_value("cluster.yaml")
        .takes_value(true);
    let vdisks_count = Arg::with_name("vdisks_count")
        .short("d")
        .help("min - equal to number of pairs node-disk")
        .takes_value(true);
    let replicas = Arg::with_name("replicas")
        .short("r")
        .default_value("1")
        .takes_value(true);
    debug!("input arg: OK");
    App::new("Config Cluster Generator")
        .arg(input)
        .arg(output)
        .arg(vdisks_count)
        .arg(replicas)
        .get_matches()
}
