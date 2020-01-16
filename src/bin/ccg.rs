#[macro_use]
extern crate log;

use bob::configs::cluster::{Config, Node, Replica, VDisk};
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
    if let Some(output) = read_from_file().and_then(|input| generate_config(input)) {
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

#[derive(Debug)]
struct Pair {
    node: String,
    disk: String,
    used_count: AtomicUsize,
}

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

fn lcm(a: usize, b: usize) -> usize {
    let lcm = a / gcd(a, b) * b;
    debug!("lcm of {} and {} is {}", a, b, lcm);
    lcm
}

fn simple_gen(mut config: Config, replicas_count: usize, vdisks_count: usize) -> Config {
    let mut pairs = get_pairs(&config);
    let vdisks_count = vdisks_count.max(lcm(pairs.len(), replicas_count));
    debug!("new vdisks count: OK [{}]", vdisks_count);
    let mut vdisks = Vec::new();
    while vdisks.len() < vdisks_count {
        let mut vdisk = VDisk {
            id: Some(vdisks.len() as i32),
            replicas: Vec::new(),
        };
        pairs.sort_by(|a, b| a.used_count.load(ORD).cmp(&b.used_count.load(ORD)));
        let mut iter = pairs.iter().cycle();
        while vdisk.replicas.len() < replicas_count {
            if let Some(pair) = iter.next() {
                vdisk.replicas.push(Replica {
                    node: Some(pair.node.clone()),
                    disk: Some(pair.disk.clone()),
                });
                pair.used_count.fetch_add(1, ORD);
                debug!("replica added: {} {}", pair.node, pair.disk);
            }
        }
        debug!("vdisk added: {}", vdisk.id());
        vdisks.push(vdisk);
    }
    config.vdisks = vdisks;
    debug!("simple gen: OK [\n{:#?}\n]", pairs);
    config
}

fn get_pairs(config: &Config) -> Vec<Pair> {
    config
        .nodes
        .iter()
        .flat_map(|node| {
            let node_name = node.name();
            node.disks.iter().map(move |d| Pair {
                node: node_name.clone(),
                disk: d.name.clone().unwrap(),
                used_count: AtomicUsize::new(0),
            })
        })
        .collect()
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

fn get_vdisks_count(nodes: &[Node]) -> Option<usize> {
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

fn get_pairs_count(nodes: &[Node]) -> usize {
    nodes.iter().fold(0, |acc, n| acc + n.disks.len())
}

fn write_to_file(output: Config) {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open("cluster.gen-output.yaml")
        .unwrap();
    let mut output = serde_yaml::to_string(&output).unwrap();
    output += "\n";
    file.write_all(output.as_bytes()).unwrap();
    debug!("write to file: OK");
}

fn get_matches() -> ArgMatches<'static> {
    let input = Arg::with_name("input")
        .short("i")
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
        .arg(vdisks_count)
        .arg(replicas)
        .get_matches()
}
