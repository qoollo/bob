#[macro_use]
extern crate log;

use chrono::Local;
use clap::{App, Arg};
use env_logger::fmt::Color;
use log::{Level, LevelFilter};
use std::fs::{File, OpenOptions};
use std::io::Write;

#[tokio::main]
async fn main() {
    init_logger();
    if let Some(input) = read_from_file() {
        let output = generate_config(input);
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

fn read_from_file() -> Option<()> {
    let name = get_name();
    let file = open_file(name)?;
    let content = read_file(file);
    let res = deserialize(content);
    debug!("read from file: OK");
    Some(res)
}

fn get_name() -> String {
    let input = Arg::with_name("input")
        .short("i")
        .default_value("cluster.yaml")
        .takes_value(true);
    debug!("input arg: OK");
    let matches = App::new("Config Cluster Generator")
        .arg(input)
        .get_matches();
    debug!("get matches: OK");
    let name = matches
        .value_of("input")
        .expect("is some, because of default arg value");
    debug!("get name: {}", name);
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

fn read_file(file: File) -> () {
    debug!("read file: OK");
}

fn deserialize(content: ()) -> () {
    debug!("deserialize: OK");
}

fn generate_config(input: ()) -> () {
    debug!("generate config: OK");
}

fn write_to_file(output: ()) {
    debug!("write to file: OK");
}
