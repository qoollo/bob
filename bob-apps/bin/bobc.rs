#[macro_use]
extern crate log;

use bob::{Blob, BlobKey, BlobMeta, BobApiClient, GetRequest, PutRequest};
use clap::{App, Arg, ArgMatches, SubCommand};
use http::Uri;
use log::LevelFilter;
use std::fmt::Debug;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::Request;

use regex::Regex;
use std::path::PathBuf;

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

    fn from_matches(matches: &ArgMatches) -> Self {
        Self {
            port: matches.value_or_default("port"),
            target: matches.value_or_default("host"),
        }
    }
}

const KEY_ARG: &str = "key";
const KEY_SIZE_ARG: &str = "keysize";
const HOST_ARG: &str = "host";
const PORT_ARG: &str = "port";
const FILE_ARG: &str = "file";

const PUT_SC: &str = "put";
const GET_SC: &str = "get";
const EXISTS_SC: &str = "exists";

struct KeyName {
    key: Vec<u8>,
    name: String,
}

#[tokio::main]
async fn main() {
    env_logger::builder().filter_level(LevelFilter::Info).init();
    let matches = get_matches();
    if let (sc, Some(sub_mathes)) = matches.subcommand() {
        match sc {
            PUT_SC | GET_SC => {
                let file_pattern = sub_mathes.value_of(FILE_ARG).unwrap();

                let mut keys_names = Vec::new();

                match parse_filename(file_pattern) {
                    ParsedFile::WithoutRE => {
                        info!("no re");
                        let key_size = sub_mathes.value_or_default(KEY_SIZE_ARG);
                        let key = match sub_mathes.value_of(KEY_ARG) {
                            None => {
                                error!("Key arg is required if not using file pattern");
                                return;
                            }
                            Some(k) => match k.to_string().parse() {
                                Ok(k) => k,
                                Err(e) => {
                                    error!("{:?}", e);
                                    return;
                                }
                            },
                        };
                        let key = get_key_value(key, key_size);
                        keys_names.push(KeyName {
                            key,
                            name: file_pattern.to_string(),
                        });
                    }
                    ParsedFile::WithRE => {
                        info!("re");

                        if sub_mathes.is_present(KEY_ARG) {
                            error!("Either file pattern or key argument must be used")
                        }
                        return;
                    }
                }

                let addr = NetConfig::from_matches(&sub_mathes).get_uri();
                match sc {
                    PUT_SC => {
                        for kn in keys_names {
                            info!(
                                "PUT key: \"{:?}\" to \"{}\" from file \"{}\"",
                                kn.key, addr, &kn.name
                            );
                            put(kn.key, &kn.name, addr.clone()).await;
                        }

                    }
                    GET_SC => {
                        for kn in keys_names {
                            info!(
                            "GET key:\"{:?}\" from  \"{}\" to file \"{}\"",
                            kn.key, addr, &kn.name
                            );
                        get(kn.key, &kn.name, addr.clone()).await;
                        }
                    }
                    _ => {}
                }
            }
            EXISTS_SC => {
                info!("exists cmd")
            }
            _ => {}
        }
    }
}

enum ParsedFile {
    WithRE,
    WithoutRE,
}

fn parse_filename(filename: &str) -> ParsedFile {
    let path = PathBuf::from(filename);
    let name = path.file_name().unwrap().to_str().unwrap();

    let re = Regex::new("\\{key\\}").unwrap();
    let two_or_more = Regex::new(".*\\{key\\}.*\\{key\\}.*").unwrap();

    if re.is_match(name) && !two_or_more.is_match(name) {
        ParsedFile::WithRE
    } else {
        ParsedFile::WithoutRE
    }
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
                .expect("msg: &str")
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

fn get_matches<'a>() -> ArgMatches<'a> {
    let key_arg = Arg::with_name(KEY_ARG)
        .short("k")
        .long("key")
        .value_name("KEY")
        .takes_value(true);
    let key_size_arg = Arg::with_name(KEY_SIZE_ARG)
        .help("Size of the binary key")
        .takes_value(true)
        .long("size")
        .short("s")
        .default_value(option_env!("BOB_KEY_SIZE").unwrap_or("8"));
    let host_arg = Arg::with_name(HOST_ARG)
        .long("host")
        .value_name("HOST")
        .takes_value(true)
        .default_value("127.0.0.1");
    let port_arg = Arg::with_name(PORT_ARG)
        .long("port")
        .value_name("PORT")
        .takes_value(true)
        .default_value("20000");
    let file_arg = Arg::with_name(FILE_ARG)
        .short("f")
        .long("file")
        .takes_value(true)
        .value_name("FILE")
        .required(true);
    let put_sc = SubCommand::with_name(PUT_SC)
        .arg(&key_arg)
        .arg(&key_size_arg)
        .arg(&host_arg)
        .arg(&port_arg)
        .arg(file_arg.clone().help("Input file"));
    let get_sc = SubCommand::with_name(GET_SC)
        .arg(&key_arg)
        .arg(&key_size_arg)
        .arg(&host_arg)
        .arg(&port_arg)
        .arg(file_arg.help("Output file"));
    let exists_sc = SubCommand::with_name(EXISTS_SC)
        .arg(key_arg.required(true))
        .arg(key_size_arg)
        .arg(host_arg)
        .arg(port_arg);
    App::new("bobc")
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
