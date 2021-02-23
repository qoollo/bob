use bob::configs::cluster::Cluster as ClusterConfig;
use chrono::Local;
use env_logger::fmt::Color;
use log::{Level, LevelFilter};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};

pub fn init_logger() {
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

pub fn read_config_from_file(name: &str) -> Option<ClusterConfig> {
    let file = open_file(name)?;
    let content = read_file(file)?;
    let config = deserialize(content)?;
    debug!("read from file: OK");
    Some(config)
}

fn open_file(name: &str) -> Option<File> {
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

pub fn read_file(mut file: File) -> Option<String> {
    let mut buf = String::new();
    file.read_to_string(&mut buf)
        .map(|n| debug!("read file: OK [{}b]", n))
        .map_err(|e| error!("read file: ERR [{}]", e))
        .ok()?;
    Some(buf)
}

pub fn write_to_file(mut output: String, name: String) {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(name)
        .expect("File IO error");
    output += "\n";
    file.write_all(output.as_bytes()).expect("File IO error");
    debug!("write to file: OK");
}

fn deserialize(content: String) -> Option<ClusterConfig> {
    serde_yaml::from_str(content.as_str())
        .map(|c: ClusterConfig| {
            debug!("deserialize: OK [nodes count: {}]", c.nodes().len());
            c
        })
        .map_err(|e| error!("deserialize: ERR [{}]", e))
        .ok()
}

/// greatest common divider
pub fn gcd(a: usize, b: usize) -> usize {
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
pub fn lcm(a: usize, b: usize) -> usize {
    let lcm = a / gcd(a, b) * b;
    debug!("lcm of {} and {} is {}", a, b, lcm);
    lcm
}
