mod config_cluster_generator;

#[macro_use]
extern crate log;

use anyhow::{anyhow, Result as AnyResult};
use bob::{ClusterConfig};
use clap::{App, Arg, ArgMatches, SubCommand};
use config_cluster_generator::{
    center::{check_expand_configs, get_new_disks, get_new_racks, Center},
    utils::{init_logger, ceil, read_config_from_file, write_to_file},
};

#[tokio::main]
async fn main() {
    match try_main() {
        Ok(_) => {
            info!("Exiting");
        }
        Err(err) => {
            error!("Error occured: {}", err);
        }
    }
}

fn try_main() -> AnyResult<()> {
    init_logger();
    match get_matches().subcommand() {
        ("new", Some(matches)) => subcommand_new(matches),
        ("expand", Some(matches)) => subcommand_expand(matches),
        _ => Err(anyhow!("incorrect arguments: ERR")),
    }
}

fn subcommand_new(matches: &ArgMatches) -> AnyResult<()> {
    debug!("start new config generation");
    debug!("arguments: {:?}", matches);
    let config = read_config_from_file(&get_input_config_name(matches))?;
    let output = generate_config(matches, config)?;
    let output = serde_yaml::to_string(&output).expect("config serialization error");
    debug!("config cluster generation: OK");
    if let Some(name) = matches.value_of("output") {
        write_to_file(output, name.to_owned());
        debug!("output to file: OK");
    } else {
        println!("{}", output);
        debug!("no file provided, stdout print: OK");
    }
    Ok(())
}

fn subcommand_expand(matches: &ArgMatches) -> AnyResult<()> {
    debug!("start config extending with new hardware configuration");
    debug!("arguments: {:?}", matches);
    let config = read_config_from_file(&get_input_config_name(matches))?;
    let hardware_config = read_config_from_file(&get_hardware_config_name(matches))?;
    let output = expand_config(matches, config, hardware_config)?;
    let output = serde_yaml::to_string(&output).expect("config serialization error");
    debug!("config cluster extending: OK");
    if let Some(name) = matches.value_of("output") {
        write_to_file(output, name.to_owned());
        debug!("output to file: OK");
    } else {
        println!("{}", output);
        debug!("no file provided, stdout print: OK");
    }
    Ok(())
}

fn generate_config(matches: &ArgMatches, input: ClusterConfig) -> AnyResult<ClusterConfig> {
    let replicas_count = get_replicas_count(matches)?;
    let vdisks_count = get_vdisks_count(matches)?;
    let vdisks_per_disk = vdisks_per_disk_match(matches);
    let use_racks = get_use_racks(matches);
    let res = simple_gen(
        input,
        replicas_count,
        vdisks_count,
        vdisks_per_disk,
        use_racks,
    )?;
    debug!("generate config: OK");
    Ok(res)
}

fn expand_config(
    matches: &ArgMatches,
    config: ClusterConfig,
    hardware_config: ClusterConfig,
) -> AnyResult<ClusterConfig> {
    let use_racks = get_use_racks(matches);
    let res = simple_expand(config, hardware_config, use_racks)?;
    debug!("expand config: OK");
    Ok(res)
}

fn simple_expand(
    config: ClusterConfig,
    mut hardware_config: ClusterConfig,
    use_racks: bool,
) -> AnyResult<ClusterConfig> {
    let mut center = Center::from_cluster_config(&hardware_config, use_racks)?;
    center.validate()?;
    let old_center = Center::from_cluster_config(&config, use_racks)?;
    old_center.validate()?;
    check_expand_configs(&old_center, &center, use_racks)?;

    let new_disks = get_new_disks(config.nodes(), hardware_config.nodes());
    let new_racks = if !use_racks {
        vec![]
    } else {
        get_new_racks(config.racks(), hardware_config.racks())
    };
    center.mark_new(&new_disks, &new_racks);

    let vdisks_count = config.vdisks().len();
    debug!("vdisks count: OK [{}]", vdisks_count);
    let mut vdisks = Vec::with_capacity(vdisks_count);
    for vdisk in config.vdisks() {
        let vdisk = center.create_vdisk_from_another(vdisk)?;
        debug!("vdisk added: {}", vdisk.id());
        vdisks.push(vdisk);
    }
    hardware_config.vdisks_extend(vdisks);
    debug!("extend config: OK [\n{:#?}\n]", center);
    Ok(hardware_config)
}

fn simple_gen(
    mut config: ClusterConfig,
    replicas_count: usize,
    mut vdisks_count: usize,
    vdisks_per_disk: bool,
    use_racks: bool,
) -> AnyResult<ClusterConfig> {
    let mut center = Center::from_cluster_config(&config, use_racks)?;
    center.validate()?;
    if vdisks_per_disk {
        vdisks_count = ceil(vdisks_count * center.disks_count(), replicas_count);
    }

    debug!("new vdisks count: OK [{}]", vdisks_count);
    let mut vdisks = Vec::new();
    while vdisks.len() < vdisks_count {
        let vdisk = center.create_vdisk(vdisks.len() as u32, replicas_count)?;
        debug!("vdisk added: {}", vdisk.id());
        vdisks.push(vdisk);
    }
    config.vdisks_extend(vdisks);
    debug!("simple gen: OK [\n{:#?}\n]", center);
    Ok(config)
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
        .expect("is some, because of default arg value");
    debug!("get hardware config name: OK [{}]", name);
    name.to_owned()
}

fn get_use_racks(matches: &ArgMatches) -> bool {
    let res = matches.is_present("use_racks");
    debug!("get_use_racks: OK [{}]", res);
    res
}

fn get_replicas_count(matches: &ArgMatches) -> AnyResult<usize> {
    matches
        .value_of("replicas")
        .expect("replicas count")
        .parse()
        .map_err(|err| anyhow!("get replicas count: {}", err))
}

fn get_vdisks_count(matches: &ArgMatches) -> AnyResult<usize> {
    if let Some(s) = matches.value_of("vdisks_count") {
        s.parse()
            .map_err(|err| anyhow!("get vdisks count: {}", err))
    } else if let Some(s) = matches.value_of("vdisks_per_disk") {
        s.parse()
            .map_err(|err| anyhow!("get vdisks per disk: {}", err))
    } else {
        Err(anyhow!("No -p or -d argument specified"))
    }
}

fn vdisks_per_disk_match(matches: &ArgMatches) -> bool {
    let res = matches.is_present("vdisks_per_disk");
    debug!("vdisks_per_disk_match: OK [{}]", res);
    res
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
        .conflicts_with("vdisks_per_disk")
        .takes_value(true);
    let vdisks_per_disk = Arg::with_name("vdisks_per_disk")
        .short("p")
        .help("number of vdisks per physical disk")
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
    let use_racks = Arg::with_name("use_racks")
        .short("R")
        .long("use-racks")
        .help("Use racks field in config")
        .takes_value(false);
    debug!("input arg: OK");
    let subcommand_expand = SubCommand::with_name("expand")
        .arg(input.clone())
        .arg(output.clone())
        .arg(use_racks.clone())
        .arg(hardware_config);

    let subcommand_new = SubCommand::with_name("new")
        .arg(input)
        .arg(output)
        .arg(vdisks_per_disk)
        .arg(vdisks_count)
        .arg(use_racks)
        .arg(replicas);

    App::new("Config Cluster Generator")
        .subcommand(subcommand_expand)
        .subcommand(subcommand_new)
        .get_matches()
}
