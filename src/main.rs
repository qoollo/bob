use anyhow::Result as AnyResult;
use clap::{App, Arg, ArgMatches};
use std::{
    collections::HashMap, fmt::Write as FMTWrite, fs::OpenOptions, io::Write as IOWrite,
    path::PathBuf,
};

const SOURCE_PATH_OPT: &str = "source path";
const DESTINATION_PATH_OPT: &str = "destination path";
const OUTPUT_PATH_OPT: &str = "output path";
const TIMESTAMP_TO_OPT: &str = "timestamp to";
const BLOB_SUFFIX_OPT: &str = "blob suffix";

fn main() {
    match try_main() {
        Ok(_) => {}
        Err(err) => {
            log::error!("Program exited with error: {}", err);
        }
    }
}

fn try_main() -> AnyResult<()> {
    env_logger::init();
    let settings = Settings::from_matches()?;

    let blob_dirs = get_blob_dirs(&settings, &settings.source_path, &"".into())?;

    let (total_dirs, total_blobs) = blob_dirs
        .iter()
        .fold((0, 0), |(dirs, blobs), (_, x)| (dirs + 1, blobs + x));
    let (filtered_dirs, filtered_blobs) = blob_dirs
        .iter()
        .filter(|(d, _)| should_move(d, settings.timestamp_to))
        .fold((0, 0), |(dirs, blobs), (_, x)| (dirs + 1, blobs + x));
    println!("Move {} of {} blobs", filtered_blobs, total_blobs);
    println!("Move {} of {} dirs", filtered_dirs, total_dirs);

    let mut result = String::new();
    writeln!(result, "#!/usr/bin/bash -v")?;

    for (parent, mut dirs) in blob_dirs
        .keys()
        .filter(|d| should_move(d, settings.timestamp_to))
        .fold(HashMap::new(), |mut map, d| {
            if let Some(parent) = d.parent() {
                map.entry(parent.clone()).or_insert(vec![]).push(d);
            }
            map
        })
    {
        writeln!(result)?;
        let mut full_parent = settings.destination_path.clone();
        full_parent.extend(parent.components());
        writeln!(result, "mkdir -p {}", full_parent.to_str().unwrap())?;
        dirs.sort_by_cached_key(|x| x.to_str().unwrap());
        for dir in dirs {
            let mut src = settings.source_path.clone();
            src.extend(dir.components());
            let mut dst = settings.destination_path.clone();
            dst.extend(dir.components());
            writeln!(
                result,
                "mv {} {}",
                src.to_str().unwrap(),
                dst.parent().unwrap().to_str().unwrap()
            )?;
        }
    }

    let mut output_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(settings.output_path)?;
    output_file.write_all(result.as_bytes())?;
    Ok(())
}

fn should_move(path: &PathBuf, timestamp_to: u64) -> bool {
    let timestamp = match path
        .file_name()
        .and_then(|path| path.to_str())
        .and_then(|path| path.split("_").next())
        .and_then(|ts| ts.parse::<u64>().ok())
    {
        Some(path) => path,
        None => return false,
    };
    if timestamp <= timestamp_to {
        return true;
    }
    false
}

fn get_blob_dirs(
    settings: &Settings,
    base_dir: &PathBuf,
    path: &PathBuf,
) -> AnyResult<HashMap<PathBuf, u64>> {
    let mut result = HashMap::new();
    let mut full_path = base_dir.clone();
    full_path.extend(path.components());
    for entry in full_path.read_dir()?.filter_map(|entry| entry.ok()) {
        match entry.file_type()? {
            file_type if file_type.is_dir() => {
                let mut path = path.clone();
                path.push(entry.file_name());
                result.extend(get_blob_dirs(&settings, &base_dir, &path)?);
            }
            file_type if file_type.is_file() => {
                if entry
                    .file_name()
                    .to_str()
                    .unwrap()
                    .ends_with(&settings.blob_suffix)
                {
                    *result.entry(path.clone()).or_default() += 1;
                }
            }
            _ => {}
        }
    }
    Ok(result)
}

#[derive(Debug)]
struct Settings {
    source_path: PathBuf,
    destination_path: PathBuf,
    timestamp_to: u64,
    blob_suffix: String,
    output_path: PathBuf,
}

impl Settings {
    fn from_matches() -> AnyResult<Self> {
        let matches = get_matches();
        Ok(Self {
            source_path: matches.value_of(SOURCE_PATH_OPT).expect("required").into(),
            destination_path: matches
                .value_of(DESTINATION_PATH_OPT)
                .expect("required")
                .into(),
            output_path: matches.value_of(OUTPUT_PATH_OPT).expect("required").into(),
            timestamp_to: matches
                .value_of(TIMESTAMP_TO_OPT)
                .expect("required")
                .parse()?,
            blob_suffix: matches.value_of(BLOB_SUFFIX_OPT).expect("default").into(),
        })
    }
}

fn get_matches() -> ArgMatches<'static> {
    App::new("Bob move")
        .arg(
            Arg::with_name(SOURCE_PATH_OPT)
                .short("s")
                .long("source-path")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name(DESTINATION_PATH_OPT)
                .short("d")
                .long("destination-path")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name(OUTPUT_PATH_OPT)
                .short("o")
                .long("output")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name(TIMESTAMP_TO_OPT)
                .short("t")
                .long("timestamp-to")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name(BLOB_SUFFIX_OPT)
                .short("b")
                .long("blob-suffix")
                .default_value(".blob")
                .takes_value(true),
        )
        .get_matches()
}
