#[macro_use]
extern crate log;

use bob::{Blob, BlobKey, BlobMeta, BobApiClient, ExistRequest, GetRequest, PutRequest};
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use http::Uri;
use lazy_static::lazy_static;
use log::LevelFilter;
use regex::Regex;
use std::fmt::Debug;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::{read_dir, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::Request;

struct NetConfig {
    port: u16,
    target: String,
}
impl NetConfig {
    fn get_uri(&self) -> http::Uri {
        http::Uri::builder()
            .scheme("http")
            .authority(format!("{}:{}", self.target, self.port).as_str())
            .path_and_query("/")
            .build()
            .unwrap()
    }

    fn from_args(args: &AppArgs) -> Self {
        Self {
            port: args.port,
            target: args.host.clone(),
        }
    }
}

const KEY_ARG: &str = "key";
const KEY_SIZE_ARG: &str = "key-size";
const HOST_ARG: &str = "host";
const PORT_ARG: &str = "port";
const FILE_ARG: &str = "file";

const PUT_SC: &str = "put";
const GET_SC: &str = "get";
const EXISTS_SC: &str = "exists";

#[derive(Debug)]
enum ParseError {
    FilePattern,
    KeyPattern,
}

#[derive(Debug)]
struct AppArgs<'a> {
    subcommand: &'a str,
    file_pattern: Option<FilePattern<'a>>,
    key_pattern: Option<KeyPattern>,
    keysize: usize,
    host: String,
    port: u16,
}

impl<'a> AppArgs<'a> {
    fn from_matches(matches: &'a ArgMatches<'a>) -> Self {
        let (subcommand, sub_matches) = matches.subcommand();
        let sub_matches = sub_matches.unwrap();
        let file_pattern = match sub_matches.value_of(FILE_ARG) {
            None => None,
            Some(p) => match Self::parse_file_pattern(p) {
                Ok(val) => Some(val),
                Err(e) => panic!("{:?}", e), // should send an error to the caller
            },
        };
        let key_pattern = match sub_matches.value_of(KEY_ARG) {
            None => None,
            Some(p) => match Self::parse_key_pattern(p) {
                Ok(val) => Some(val),
                Err(e) => panic!("{:?}", e), // again
            },
        };

        Self {
            subcommand,
            file_pattern,
            key_pattern,
            keysize: sub_matches.value_or_default(KEY_SIZE_ARG),
            host: sub_matches.value_or_default(HOST_ARG),
            port: sub_matches.value_or_default(PORT_ARG),
        }
    }
    fn parse_file_pattern(file_pattern: &str) -> Result<FilePattern<'_>, ParseError> {
        let path = PathBuf::from(file_pattern);
        let name = path.file_name().unwrap().to_str().unwrap();

        let re = Regex::new("\\{key\\}").unwrap();

        match re.find_iter(name).count() {
            0 => Ok(FilePattern::WithoutRE(file_pattern)),
            1 => {
                let path = PathBuf::from(file_pattern);

                let dir = match path.parent().unwrap() {
                    p if p == Path::new("") => Path::new("."),
                    p => p,
                };
                Ok(FilePattern::WithRE(
                    RePattern::new(file_pattern),
                    dir.to_owned(),
                ))
                
            },
            _ => Err(ParseError::FilePattern),
        }
    }
    fn parse_key_pattern(key_arg_cont: &str) -> Result<KeyPattern, ParseError> {
        if let Ok(key) = key_arg_cont.parse() {
            return Ok(KeyPattern::Single(key));
        }
        if key_arg_cont.contains("-") {
            let v: Vec<&str> = key_arg_cont.split("-").collect();
            if v.len() != 2 || v[0] >= v[1] {
                return Err(ParseError::KeyPattern);
            }
            let range = v[0].parse().unwrap()..=v[1].parse().unwrap();
            return Ok(KeyPattern::Range(range));
        }
        Ok(KeyPattern::Multiple(
            key_arg_cont
                .split(",")
                .map(|n| n.parse().unwrap())
                .collect(),
        ))
    }
}

#[derive(Debug)]
enum FilePattern<'a> {
    WithRE(RePattern<'a>, PathBuf),
    WithoutRE(&'a str),
}

#[derive(Debug)]
struct RePattern<'a> {
    inner: &'a str,
}

impl<'a> RePattern<'a> {
    fn new(f: &'a str) -> Self {
        Self { inner: f }
    }

    fn get_regex(&self) -> Regex {
        lazy_static! {
            static ref RE: Regex = Regex::new("\\\\\\{key\\\\\\}").unwrap();
        }
        let st = RE
            .replace(&regex::escape(self.inner), "(\\d+)")
            .into_owned();
        Regex::new(&st).unwrap()
    }
    fn get_filenames(&self, keys: &KeyPattern) -> Vec<(u64, String)> {
        lazy_static! {
            static ref RE: Regex = Regex::new("\\{key\\}").unwrap();
        }
        match keys {
            KeyPattern::Single(val) => {
                vec![(*val, RE.replace(self.inner, val.to_string()).into_owned())]
            }
            KeyPattern::Range(r) => {
                r.clone() // idk how to use map Range without moving out a value
                    .map(|n| (n, RE.replace(self.inner, n.to_string()).into_owned()))
                    .collect()
            }
            KeyPattern::Multiple(v) => v
                .into_iter()
                .map(|n| (*n, RE.replace(self.inner, n.to_string()).into_owned()))
                .collect(),
        }
    }
}

#[derive(Debug, PartialEq)]
enum KeyPattern {
    Single(u64),
    Range(RangeInclusive<u64>),
    Multiple(Vec<u64>),
}

#[derive(Debug)]
struct KeyName {
    key: Vec<u8>,
    name: String,
}

#[tokio::main]
async fn main() {
    env_logger::builder().filter_level(LevelFilter::Info).init();
    let matches = get_matches();
    let app_args = AppArgs::from_matches(&matches);

    let addr = NetConfig::from_args(&app_args).get_uri();
    match app_args.subcommand {
        PUT_SC => {
            let keys_names = match app_args.file_pattern {
                Some(FilePattern::WithRE(re, dir)) => {
                    // maybe create filenames from pattern AND keys and try to send them?
                    if app_args.key_pattern != None {
                        return error!("Either file pattern or key argument must be used");
                    }
                    prepare_put_from_pattern(&re.get_regex(), &dir, app_args.keysize).await
                }
                Some(FilePattern::WithoutRE(path)) => match app_args.key_pattern {
                    None => {
                        return error!("Key arg is required if not using file pattern");
                    }
                    Some(p) => match p {
                        KeyPattern::Single(key) => {
                            let key = get_key_value(key, app_args.keysize);
                            vec![KeyName {
                                key,
                                name: path.to_string(),
                            }]
                        }
                        _ => return error!("Multiple keys are not allowed without pattern"),
                    },
                },

                None => unreachable!(),
            };
            for kn in keys_names {
                info!(
                    "PUT key: \"{:?}\" to \"{}\" from file \"{}\"",
                    kn.key, addr, &kn.name
                );
                put(kn.key, &kn.name, addr.clone()).await; // i dont like this clone() but BobApiClient::connect requires owning
            }
        }
        GET_SC => {
            let keys_names = match app_args.file_pattern.unwrap() {
                FilePattern::WithRE(re, _) => {
                    if app_args.key_pattern == None {
                        return error!("Key arg is required when using pattern");
                    }

                    let files = re.get_filenames(&app_args.key_pattern.unwrap());
                    let keys_names = prepare_get(files, app_args.keysize);
                    keys_names
                }
                FilePattern::WithoutRE(path) => match app_args.key_pattern.unwrap() {
                    KeyPattern::Single(val) => {
                        prepare_get(vec![(val, path.to_string())], app_args.keysize)
                    }
                    KeyPattern::Range(_) | KeyPattern::Multiple(_) => {
                        return error!("Multiple keys are not allowed without pattern")
                    }
                },
            };
            for kn in keys_names {
                info!(
                    "GET key:\"{:?}\" from  \"{}\" to file \"{}\"",
                    kn.key, addr, &kn.name
                );
                get(kn.key, &kn.name, addr.clone()).await; // i dont like this clone()...
            }
        }
        EXISTS_SC => {
            match app_args.key_pattern.unwrap() {
                KeyPattern::Single(val) => {
                    let key = get_key_value(val, app_args.keysize);
                    exist(vec![key], addr).await;
                }
                KeyPattern::Range(range) => {
                    let keysize = app_args.keysize;
                    let keys = range.map(|v| get_key_value(v, keysize)).collect();
                    exist(keys, addr).await;
                }
                KeyPattern::Multiple(vec) => {
                    let keysize = app_args.keysize;
                    let keys = vec.into_iter().map(|v| get_key_value(v, keysize)).collect();
                    exist(keys, addr).await;
                }
            }
        }
        _ => unreachable!(),
    }
}

async fn prepare_put_from_pattern(re_path: &Regex, dir: &PathBuf, key_size: usize) -> Vec<KeyName> {
    let mut dir_iter = read_dir(dir).await.unwrap();

    let mut keys_names = Vec::new();
    while let Some(entry) = dir_iter.next_entry().await.unwrap() {
        let file = entry.path();
            let name = file.to_str().unwrap().to_owned();
            if let (false, Some(cap)) = (file.is_dir(), re_path.captures(&name)) {
                let key = get_key_value(cap[1].parse().unwrap(), key_size);
                keys_names.push(KeyName { key, name });
            }
    }
    keys_names
}

fn prepare_get(filenames: Vec<(u64, String)>, key_size: usize) -> Vec<KeyName> {
    let mut keys_names = Vec::with_capacity(filenames.len());
    for (i, name) in filenames {
        keys_names.push(KeyName {
            key: get_key_value(i, key_size),
            name,
        })
    }
    keys_names
}

async fn put(key: Vec<u8>, filename: &str, addr: Uri) {
    let file = File::open(filename).await;

    match file {
        Err(e) => {
            error!("{:?}", e);
        }
        Ok(mut f) => {
            let mut client = BobApiClient::connect(addr).await.unwrap();

            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let meta = BlobMeta { timestamp };

            let mut data = Vec::new();
            match f.read_to_end(&mut data).await {
                Ok(_) => {}
                Err(e) => {
                    error!("{:?}", e);
                    return;
                }
            }
            let blob = Blob {
                data,
                meta: Some(meta),
            };
            let message = PutRequest {
                key: Some(BlobKey { key }),
                data: Some(blob),
                options: None,
            };
            let put_req = Request::new(message);

            let res = client.put(put_req).await;
            info!("{:#?}", res);
        }
    }
}

async fn get(key: Vec<u8>, filename: &str, addr: Uri) {
    let mut client = BobApiClient::connect(addr).await.unwrap();

    let message = GetRequest {
        key: Some(BlobKey { key }),
        options: None,
    };
    let get_req = Request::new(message);
    let res = client.get(get_req).await;
    match res {
        Ok(res) => {
            let res: tonic::Response<_> = res;
            let data: &Blob = res.get_ref();
            info!("response meta: {:?})", res.metadata());
            info!("data len: {}, meta: {:?}", data.data.len(), data.meta);
            let file = File::create(filename).await;
            match file {
                Ok(mut file) => match file.write_all(&data.data).await {
                    Ok(()) => {}
                    Err(e) => {
                        error!("{:?}", e);
                    }
                },
                Err(err) => {
                    error!("{:?}", err);
                }
            }
        }
        Err(res) => {
            error!("{:?}", res);
        }
    }
}

async fn exist(keys: Vec<Vec<u8>>, addr: Uri) {
    let mut client = BobApiClient::connect(addr).await.unwrap();

    let keys = keys.into_iter().map(|key| BlobKey { key }).collect();
    let message = ExistRequest {
        keys,
        options: None,
    };
    let request = Request::new(message);
    let res = client.exist(request).await;
    match res {
        Ok(res) => {
            info!("{:?}", res);
        }
        Err(e) => {
            error!("{:?}", e);
        }
    }
}

fn get_matches<'a>() -> ArgMatches<'a> {
    let key_arg = Arg::with_name(KEY_ARG)
        .short("k")
        .long(KEY_ARG)
        .value_name("KEY")
        .takes_value(true);
    let key_size_arg = Arg::with_name(KEY_SIZE_ARG)
        .help("Size of the binary key")
        .long(KEY_SIZE_ARG)
        .value_name("KEY-SIZE")
        .takes_value(true)
        .default_value(option_env!("BOB_KEY_SIZE").unwrap_or("8"));
    let host_arg = Arg::with_name(HOST_ARG)
        .long(HOST_ARG)
        .value_name("HOST")
        .takes_value(true)
        .default_value("127.0.0.1");
    let port_arg = Arg::with_name(PORT_ARG)
        .long(PORT_ARG)
        .value_name("PORT")
        .takes_value(true)
        .default_value("20000");
    let file_arg = Arg::with_name(FILE_ARG)
        .short("f")
        .long(FILE_ARG)
        .takes_value(true)
        .value_name("FILE")
        .required(true);
    let put_sc = SubCommand::with_name(PUT_SC)
        .arg(&key_arg)
        .arg(&key_size_arg)
        .arg(&host_arg)
        .arg(&port_arg)
        .arg(file_arg.clone().help("Input file"));
    let key_arg = key_arg.required(true);
    let get_sc = SubCommand::with_name(GET_SC)
        .arg(&key_arg)
        .arg(&key_size_arg)
        .arg(&host_arg)
        .arg(&port_arg)
        .arg(file_arg.help("Output file"));
    let exists_sc = SubCommand::with_name(EXISTS_SC)
        .arg(key_arg)
        .arg(key_size_arg)
        .arg(host_arg)
        .arg(port_arg);
    App::new("bobc")
        .setting(AppSettings::ArgRequiredElseHelp)
        .subcommand(put_sc)
        .subcommand(get_sc)
        .subcommand(exists_sc)
        .get_matches()
}

fn get_key_value(key: u64, key_size: usize) -> Vec<u8> {
    info!(
        "key size: {}, as u32: {}, u64::MAX: {}",
        key_size,
        key_size as u32,
        u64::MAX
    );
    let max_allowed_key = if key_size >= 8 {
        u64::MAX
    } else {
        256_u64.pow(key_size as u32) - 1
    };
    if key > max_allowed_key {
        panic!(
            "Key {} is not allowed by keysize {} (max allowed key is {})",
            key, key_size, max_allowed_key
        );
    }
    get_key(key, key_size)
}

fn get_key(k: u64, key_size: usize) -> Vec<u8> {
    let mut data = k.to_le_bytes().to_vec();
    data.resize(key_size, 0);
    data
}

trait ValueOrDefault<'a, 'b> {
    fn value_or_default<T>(&'a self, key: &'b str) -> T
    where
        T: FromStr + Debug,
        <T as std::str::FromStr>::Err: std::fmt::Debug;
}

impl<'a, 'b> ValueOrDefault<'a, 'b> for ArgMatches<'a> {
    fn value_or_default<T>(&'a self, key: &'b str) -> T
    where
        T: FromStr + Debug,
        <T as std::str::FromStr>::Err: std::fmt::Debug,
    {
        self.value_of(key).unwrap_or_default().parse().unwrap()
    }
}
