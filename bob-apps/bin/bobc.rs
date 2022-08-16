#[macro_use]
extern crate log;

use bob::{Blob, BlobKey, BlobMeta, BobApiClient, ExistRequest, GetRequest, PutRequest};
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use log::LevelFilter;
use regex::Regex;
use tonic::transport::Channel;
use std::fmt::Debug;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};
use std::iter;
use tokio::fs;
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
struct AppArgs {
    subcommand: String,
    file_pattern: Option<FilePattern>,
    key_pattern: Option<KeyPattern>,
    keysize: usize,
    host: String,
    port: u16,
}

impl AppArgs {
    fn from_matches(matches: ArgMatches) -> Self {
        let (sc, sub_matches) = matches.subcommand();
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
            subcommand: sc.to_string(),
            file_pattern,
            key_pattern,
            keysize: sub_matches.value_or_default(KEY_SIZE_ARG),
            host: sub_matches.value_or_default(HOST_ARG),
            port: sub_matches.value_or_default(PORT_ARG),
        }
    }
    fn parse_file_pattern(file_pattern: &str) -> Result<FilePattern, ParseError> {
        let path = PathBuf::from(file_pattern);
        let name = path.file_name().unwrap().to_str().unwrap();

        let re = Regex::new("\\{key\\}").unwrap();

        match re.find_iter(name).count() {
            0 => Ok(FilePattern::WithoutRE(file_pattern.to_string())),
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
            }
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
enum FilePattern {
    WithRE(RePattern, PathBuf),
    WithoutRE(String),
}

#[derive(Debug)]
struct RePattern {
    inner: String,
}

impl RePattern {
    fn new(f: &str) -> Self {
        Self {
            inner: f.to_string(),
        }
    }

    fn get_regex(&self) -> Regex {
        let st = regex::escape(&self.inner).replace("\\{key\\}", "(\\d+)");
        Regex::new(&st).unwrap()
    }
    fn get_filenames(self, keys: &KeyPattern) -> Box<dyn Iterator<Item = (u64, String)>> {
        match keys {
            KeyPattern::Single(val) => {
                Box::new(iter::once((*val, self.inner.replace("{key}", &val.to_string()))))
            }
            KeyPattern::Range(r) => {
                Box::new(r.clone().map(move |n| (n, self.inner.replace("{key}", &n.to_string()))))
            }
            KeyPattern::Multiple(v) => Box::new(
                v.clone()
                    .into_iter()
                    .map(move |n| (n, self.inner.replace("{key}", &n.to_string()))),
            ),
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
    key: u64,
    name: String,
}

#[tokio::main]
async fn main() {
    env_logger::builder().filter_level(LevelFilter::Info).init();
    let matches = get_matches();
    let app_args = AppArgs::from_matches(matches);

    let addr = NetConfig::from_args(&app_args).get_uri();
    let mut client = BobApiClient::connect(addr).await.unwrap();
    match app_args.subcommand.as_str() {
        PUT_SC => {
            let keys_names = match (app_args.file_pattern.unwrap(), app_args.key_pattern) {
                (FilePattern::WithRE(re, dir), None) => {
                    prepare_put_from_pattern(&re.get_regex(), &dir).await
                }
                (FilePattern::WithRE(re, _), Some(key_pattern)) => {
                    Box::new(re.get_filenames(&key_pattern).map(|(key, name)| KeyName {key,name}))
                }
                (FilePattern::WithoutRE(path), Some(KeyPattern::Single(key))) => {
                    Box::new(iter::once(KeyName {
                        key,
                        name: path.to_string()
                    }))
                }
                (FilePattern::WithoutRE(path), Some(KeyPattern::Range(r))) => {
                    Box::new(r.map(move |key| KeyName {
                        key,
                        name: path.to_string()
                    }))
                }
                (FilePattern::WithoutRE(path), Some(KeyPattern::Multiple(v))) => {
                    Box::new(v.into_iter().map(move |key| KeyName {
                        key,
                        name: path.to_string()
                    }))
                }
                (FilePattern::WithoutRE(_), None) => {
                    return error!("Key arg is required if not using file pattern");
                }
            };
            for kn in keys_names {
                put(kn.key, app_args.keysize, &kn.name, &mut client).await;
            }
        }
        GET_SC => {
            let keys_names = match (app_args.file_pattern.unwrap(), app_args.key_pattern.unwrap()) {
                (FilePattern::WithRE(re, _), key) => {
                    let files = re.get_filenames(&key);
                    prepare_get(files)
                }
                (FilePattern::WithoutRE(path), KeyPattern::Single(key)) => {
                    prepare_get(Box::new(iter::once((key, path.to_string()))))
                }
                (FilePattern::WithoutRE(_), _) => {
                    return error!("Multiple keys are not allowed without pattern")
                }
            };
            for kn in keys_names {
                get(kn.key, app_args.keysize, &kn.name, &mut client).await;
            }
        }
        EXISTS_SC => {
            match app_args.key_pattern.unwrap() {
                KeyPattern::Single(val) => {
                    exist(vec![val], app_args.keysize, &mut client).await;
                }
                KeyPattern::Range(range) => {
                    exist(range.collect(), app_args.keysize, &mut client).await;
                }
                KeyPattern::Multiple(vec) => {
                    exist(vec, app_args.keysize, &mut client).await;
                }
            }
        }
        _ => unreachable!(),
    }
}

async fn prepare_put_from_pattern(re_path: &Regex, dir: &PathBuf) -> Box<dyn Iterator<Item = KeyName>> {
    let mut dir_iter = fs::read_dir(dir).await.unwrap();

    let mut keys_names = Vec::new();
    while let Some(entry) = dir_iter.next_entry().await.unwrap() {
        let file = entry.path();
            let name = file.to_str().unwrap().to_owned();
            if let (false, Some(cap)) = (file.is_dir(), re_path.captures(&name)) {
                let key = cap[1].parse().unwrap();
                keys_names.push(KeyName { key, name });
            }
    }
    Box::new(keys_names.into_iter())
}

fn prepare_get(filenames: Box<dyn Iterator<Item = (u64, String)>>) -> Box<dyn Iterator<Item = KeyName>> {
    Box::new(filenames.map(|(key, name)| KeyName {
        key,
        name
    }))
}

async fn put(key: u64, key_size: usize, filename: &str, client: &mut BobApiClient<Channel>) {
    match fs::read(filename).await {
        Ok(data) => {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let meta = BlobMeta { timestamp };
            let blob = Blob {
                data,
                meta: Some(meta),
            };
            let message = PutRequest {
                key: Some(BlobKey { key: get_key_value(key, key_size) }),
                data: Some(blob),
                options: None,
            };
            let put_req = Request::new(message);

            match client.put(put_req).await {
                Ok(_) => info!("key: {}, file: {}", key, filename),
                Err(e) => error!("{:?}", e)
            }
        }
        Err(e) => {
            error!("{:?}", e)
        }
    }
}

async fn get(key: u64, key_size: usize, filename: &str, client: &mut BobApiClient<Channel>) {
    let message = GetRequest {
        key: Some(BlobKey { key: get_key_value(key, key_size) }),
        options: None,
    };
    let get_req = Request::new(message);
    let res = client.get(get_req).await;
    match res {
        Ok(res) => {
            let res: tonic::Response<_> = res;
            let data = res.get_ref();
            match fs::write(filename, &data.data).await {
                Err(e) => error!("{:?}", e),
                _ => info!("key: {}, file: {}", key, filename)

            }
        }
        Err(res) => {
            error!("{:?}", res);
        }
    }
}

async fn exist(keys: Vec<u64>, key_size: usize, client: &mut BobApiClient<Channel>) {
    let k = keys.iter().map(|key| BlobKey { key: get_key_value(*key, key_size) }).collect();
    let message = ExistRequest {
        keys: k,
        options: None,
    };
    let request = Request::new(message);
    let res = client.exist(request).await;
    match res {
        Ok(res) => {
            let res: tonic::Response<_> = res;
            let data = res.get_ref();
            for (k, r) in iter::zip(keys, &data.exist) {
                info!("key: {}, exists: {}", k, r)
            }
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
