mod config_cluster_generator;

#[macro_use]
extern crate log;

use bob::{ClusterConfig, ClusterNodeConfig as ClusterNode};
use clap::{App, Arg, ArgMatches, SubCommand};
use config_cluster_generator::{
    center::{get_new_disks, get_pairs_count, get_structure},
    utils::{init_logger, lcm, read_config_from_file, write_to_file},
};

#[tokio::main]
async fn main() {
    init_logger();
    match get_matches().subcommand() {
        ("new", Some(matches)) => subcommand_new(matches),
        ("expand", Some(matches)) => subcommand_expand(matches),
        _ => {
            debug!("incorrect arguments: ERR");
            Some(())
        }
    };
}

fn subcommand_new(matches: &ArgMatches) -> Option<()> {
    debug!("start new config generation");
    debug!("arguments: {:?}", matches);
    let config = read_config_from_file(&get_input_config_name(matches))?;
    if let Some(output) = generate_config(matches, config) {
        let output = serde_yaml::to_string(&output).expect("config serialization error");
        debug!("config cluster generation: OK");
        if let Some(name) = matches.value_of("output") {
            write_to_file(output, name.to_owned());
            debug!("output to file: OK");
        } else {
            println!("{}", output);
            debug!("no file provided, stdout print: OK");
        }
    } else {
        debug!("config cluster generation: ERR");
    }
    Some(())
}

fn subcommand_expand(matches: &ArgMatches) -> Option<()> {
    debug!("start config extending with new hardware configuration");
    debug!("arguments: {:?}", matches);
    let config = read_config_from_file(&get_input_config_name(matches))?;
    let hardware_config = read_config_from_file(&get_hardware_config_name(matches))?;
    let output = expand_config(config, hardware_config)?;
    let output = serde_yaml::to_string(&output).expect("config serialization error");
    debug!("config cluster extending: OK");
    if let Some(name) = matches.value_of("output") {
        write_to_file(output, name.to_owned());
        debug!("output to file: OK");
    } else {
        println!("{}", output);
        debug!("no file provided, stdout print: OK");
    }
    Some(())
}

fn get_input_config_name(matches: &ArgMatches) -> String {
    let name = matches
        .value_of("input")
        .expect("is some, because of default arg value");
    debug!("get input config name: OK [{}]", name);
    name.to_owned()
}

fn get_hardware_config_name(matches: &ArgMatches) -> String {
    let name = matches
        .value_of("hardware")
        .expect("is some, because of required arg value");
    debug!("get hardware config name: OK [{}]", name);
    name.to_owned()
}

fn generate_config(matches: &ArgMatches, input: ClusterConfig) -> Option<ClusterConfig> {
    let replicas_count = get_replicas_count(matches)?;
    let vdisks_count = get_vdisks_count(matches, input.nodes())?;
    let vdisks_counts_match = vdisks_counts_match(matches);
    let res = simple_gen(input, replicas_count, vdisks_count, vdisks_counts_match);
    debug!("generate config: OK");
    Some(res)
}

fn expand_config(
    config: ClusterConfig,
    mut hardware_config: ClusterConfig,
) -> Option<ClusterConfig> {
    let mut center = get_structure(&hardware_config);
    let removed_disks: Vec<_> = get_new_disks(hardware_config.nodes(), config.nodes()).collect();
    if !removed_disks.is_empty() {
        debug!("some disks or nodes was removed: ERR {:?}", removed_disks);
        return None;
    }
    let new_disks: Vec<_> = get_new_disks(config.nodes(), hardware_config.nodes()).collect();
    center.mark_new(&new_disks);
    let vdisks_count = config.vdisks().len();
    debug!("vdisks count: OK [{}]", vdisks_count);
    let mut vdisks = Vec::with_capacity(vdisks_count);
    for vdisk in config.vdisks() {
        let vdisk = center.create_vdisk_from_another(vdisk);
        debug!("vdisk added: {}", vdisk.id());
        vdisks.push(vdisk);
    }
    hardware_config.vdisks_extend(vdisks);
    debug!("extend config: OK [\n{:#?}\n]", center);
    Some(hardware_config)
}

fn simple_gen(
    mut config: ClusterConfig,
    replicas_count: usize,
    mut vdisks_count: usize,
    vdisks_counts_match: bool,
) -> ClusterConfig {
    let center = get_structure(&config);
    if !vdisks_counts_match {
        vdisks_count = vdisks_count.max(lcm(center.disks_count(), replicas_count));
    }
    debug!("new vdisks count: OK [{}]", vdisks_count);
    let mut vdisks = Vec::new();
    while vdisks.len() < vdisks_count {
        let vdisk = center.create_vdisk(vdisks.len() as u32, replicas_count);
        debug!("vdisk added: {}", vdisk.id());
        vdisks.push(vdisk);
    }
    config.vdisks_extend(vdisks);
    debug!("simple gen: OK [\n{:#?}\n]", center);
    config
}

fn get_replicas_count(matches: &ArgMatches) -> Option<usize> {
    matches
        .value_of("replicas")
        .expect("replicas count")
        .parse()
        .map_err(|e| error!("get replicas count: ERR [{}]", e))
        .ok()
}

fn get_vdisks_count(matches: &ArgMatches, nodes: &[ClusterNode]) -> Option<usize> {
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

fn vdisks_counts_match(matches: &ArgMatches) -> bool {
    matches.is_present("exact_vdisks_count")
}

fn get_matches() -> ArgMatches<'static> {
    let input = Arg::with_name("input")
        .short("i")
        .default_value("cluster.yaml")
        .takes_value(true);
    let output = Arg::with_name("output")
        .short("o")
        .takes_value(true)
        .help("output file, if not set, use stdout instead");
    let vdisks_count = Arg::with_name("vdisks_count")
        .short("d")
        .help("min - equal to number of pairs node-disk")
        .takes_value(true);
    let replicas = Arg::with_name("replicas")
        .short("r")
        .default_value("1")
        .takes_value(true);
    let hardware_config = Arg::with_name("hardware")
        .short("H")
        .help("new hardware configuration")
        .required(true)
        .takes_value(true);
    let exact_vdisks_count = Arg::with_name("exact_vdisks_count")
        .short("e")
        .long("exact")
        .help("Create config with exactly provided vdisks count")
        .takes_value(false);
    debug!("input arg: OK");
    let subcommand_expand = SubCommand::with_name("expand")
        .arg(input.clone())
        .arg(output.clone())
        .arg(hardware_config);

    let subcommand_new = SubCommand::with_name("new")
        .arg(input)
        .arg(output)
        .arg(vdisks_count)
        .arg(exact_vdisks_count)
        .arg(replicas);

    App::new("Config Cluster Generator")
        .subcommand(subcommand_expand)
        .subcommand(subcommand_new)
        .get_matches()
}
