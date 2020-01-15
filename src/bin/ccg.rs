#[macro_use]
extern crate log;

use bob::configs::cluster::Config;
use chrono::Local;
use clap::{App, Arg, ArgMatches};
use env_logger::fmt::Color;
use log::{Level, LevelFilter};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};

#[tokio::main]
async fn main() {
    init_logger();
    if let Some(output) = read_from_file().and_then(|input| generate_config(input)) {
        let output = write_to_file(output);
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

fn generate_config(input: Config) -> Option<()> {
    let matches = get_matches();
    let vdisks_count: usize = matches
        .value_of("vdisks_count")
        .expect("is some, because of default arg value")
        .parse()
        .map_err(|e| error!("generate config: ERR [{}]", e))
        .ok()?;
    debug!("generate config: OK");
    Some(())
}

fn write_to_file(output: ()) {
    debug!("write to file: OK");
}

fn get_matches() -> ArgMatches<'static> {
    let input = Arg::with_name("input")
        .short("i")
        .default_value("cluster.yaml")
        .takes_value(true);
    debug!("input arg: OK");
    App::new("Config Cluster Generator")
        .arg(input)
        .get_matches()
}
