#[macro_use]
extern crate log;

use bob::{Blob, BlobKey, BlobMeta, BobApiClient, GetRequest, PutRequest};
use clap::{App, Arg, ArgMatches, SubCommand};
use http::Uri;
use log::LevelFilter;
use std::fmt::Debug;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::Request;

#[tokio::main]
async fn main() {
    env_logger::builder().filter_level(LevelFilter::Info).init();
    let matches = get_matches();
    if let (sc, Some(sub_mathes)) = matches.subcommand() {
        let key = get_key_value(sub_mathes);
        match sc {
            "put" => {
                let value = sub_mathes.value_of("size").unwrap();
                let size = value.parse().expect("size must be usize");
                let addr = sub_mathes
                    .value_of("uri")
                    .expect("has default value")
                    .parse()
                    .expect("wrong format of url");
                info!("PUT key: \"{:?}\" size: \"{}\" to \"{}\"", key, size, addr);
                put(key, size, addr).await;
            }
            "get" => {
                let addr = sub_mathes
                    .value_of("uri")
                    .expect("has default value")
                    .parse()
                    .expect("wrong format of url");
                info!("GET key:\"{:?}\" from  \"{}\"", key, addr);
                get(key, addr).await;
            }
            _ => {}
        }
    }
}

async fn put(key: Vec<u8>, size: usize, addr: Uri) {
    let mut client = BobApiClient::connect(addr).await.unwrap();

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("msg: &str")
        .as_secs();
    let meta = BlobMeta { timestamp };
    let blob = Blob {
        data: vec![1; size],
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

async fn get(key: Vec<u8>, addr: Uri) {
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
            info!("OK");
            info!("response meta: {:?})", res.metadata());
            info!("data len: {}, meta: {:?}", data.data.len(), data.meta);
        }
        Err(res) => {
            info!("ERR");
            info!("{:?}", res);
        }
    }
}

fn get_matches<'a>() -> ArgMatches<'a> {
    let key_arg = Arg::with_name("key").takes_value(true).required(true);
    let uri_arg = Arg::with_name("uri")
        .takes_value(true)
        .default_value("http://localhost:20000");
    let size_arg = Arg::with_name("size")
        .takes_value(true)
        .default_value("90000");
    let key_size_arg = Arg::with_name("keysize")
        .help("size of the binary key")
        .takes_value(true)
        .long("keysize")
        .short("k")
        .default_value(option_env!("BOB_KEY_SIZE").unwrap_or("8"));
    let put_sc = SubCommand::with_name("put")
        .arg(&key_arg)
        .arg(size_arg)
        .arg(&uri_arg)
        .arg(&key_size_arg);
    let get_sc = SubCommand::with_name("get")
        .arg(key_arg)
        .arg(uri_arg)
        .arg(key_size_arg);
    App::new("bobc")
        .subcommand(put_sc)
        .subcommand(get_sc)
        .get_matches()
}

fn get_key_value(matches: &'_ ArgMatches<'_>) -> Vec<u8> {
    let key_size = matches.value_or_default("keysize");
    let key = matches
        .value_of("key")
        .expect("key arg is required")
        .to_string()
        .parse()
        .expect("key must be u64");
    let max_allowed_key = 256_u64.pow(key_size as u32) - 1;
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
