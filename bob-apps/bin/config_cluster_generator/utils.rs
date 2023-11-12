use anyhow::{anyhow, Result as AnyResult};
use bob::ClusterConfig;
use chrono::Local;
use env_logger::fmt::Color;
use log::{Level, LevelFilter};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::net::Ipv4Addr;

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

pub fn read_config_from_file(name: &str) -> AnyResult<ClusterConfig> {
    let file = open_file(name)?;
    let content = read_file(file)?;
    let config = deserialize(content)?;
    debug!("read from file: OK");
    Ok(config)
}

fn open_file(name: &str) -> AnyResult<File> {
    OpenOptions::new()
        .read(true)
        .create(false)
        .open(name)
        .map(|f| {
            debug!("open file: OK");
            f
        })
        .map_err(|e| anyhow!("open file: ERR [{}]", e))
}

pub fn read_file(mut file: File) -> AnyResult<String> {
    let mut buf = String::new();
    file.read_to_string(&mut buf)
        .map(|n| debug!("read file: OK [{}b]", n))
        .map_err(|e| anyhow!("read file: ERR [{}]", e))?;
    Ok(buf)
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

fn deserialize(content: String) -> AnyResult<ClusterConfig> {
    serde_yaml::from_str(content.as_str())
        .map(|c: ClusterConfig| {
            debug!("deserialize: OK [nodes count: {}]", c.nodes().len());
            c
        })
        .map_err(|e| anyhow!("deserialize: ERR [{}]", e))
}

pub fn ceil(a: usize, b: usize) -> usize {
    if a % b > 0 {
        a / b + 1
    } else {
        a / b
    }
}

pub fn parse_address_pattern(pattern: &String) -> AnyResult<(Ipv4Addr, u16, String)> {
    let re: regex::Regex = regex::Regex::new(r"^(\d+\.\d+\.\d+\.\d+):(\d+)(/.+)$").unwrap();

    if let Some(captures) = re.captures(pattern) {
        let ip = captures.get(1).unwrap().as_str();
        let port = captures.get(2).unwrap().as_str();
        let path = captures.get(3).unwrap().as_str().to_owned();

        let ip: Ipv4Addr = ip.parse().map_err(|_| anyhow!("Failed to parse ip"))?;
        let port: u16 = port.parse().map_err(|_| anyhow!("Failed to parse port"))?;
        Ok((ip, port, path))
    } else {
        Err(anyhow!("Failed to match the pattern"))
    }
}
pub fn substitute_node(node_pattern: &String, ip: Ipv4Addr, port: u16, id: usize) -> String {
    let substituted = node_pattern
        .replace("{ip}", &ip.to_string())
        .replace("{port}", &port.to_string())
        .replace("{id}", &id.to_string());
    substituted
}

pub fn generate_range_samples(pattern: &String) -> Vec<String> {
    let re = regex::Regex::new(r"\[(\d+)-(\d+)]").unwrap();
    let ranges = re.captures_iter(pattern).map(|captures| {
        let start: usize = captures[1].parse().unwrap();
        let end: usize = captures[2].parse().unwrap();
        (start, end)
    });

    let mut samples: Vec<String> = vec![pattern.to_string()];

    for (start, end) in ranges {
        samples = samples
            .iter()
            .flat_map(|template| {
                (start..=end).map(move |i| {
                    template.replacen(&format!("[{}-{}]", start, end), &i.to_string(), 1)
                })
            })
            .collect();
    }
    samples
}
