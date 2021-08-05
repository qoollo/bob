#[macro_use]
extern crate log;

use bob::{Blob, BlobKey, BlobMeta, BobApiClient, GetRequest, PutRequest};
use clap::{App, Arg, ArgMatches, SubCommand};
use http::Uri;
use log::LevelFilter;
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
                info!("PUT key: \"{}\" size: \"{}\" to \"{}\"", key, size, addr);
                put(key, size, addr).await;
            }
            "get" => {
                let addr = sub_mathes
                    .value_of("uri")
                    .expect("has default value")
                    .parse()
                    .expect("wrong format of url");
                info!("GET key:\"{}\" from  \"{}\"", key, addr);
                get(key, addr).await;
            }
            _ => {}
        }
    }
}

async fn put(key: u64, size: usize, addr: Uri) {
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
        key: Some(BlobKey { key: get_key(key) }),
        data: Some(blob),
        options: None,
    };
    let put_req = Request::new(message);

    let res = client.put(put_req).await;
    info!("{:#?}", res);
}

async fn get(key: u64, addr: Uri) {
    let mut client = BobApiClient::connect(addr).await.unwrap();

    let message = GetRequest {
        key: Some(BlobKey { key: get_key(key) }),
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
    let put_sc = SubCommand::with_name("put")
        .arg(&key_arg)
        .arg(size_arg)
        .arg(&uri_arg);
    App::new("bobc")
        .subcommand(put_sc)
        .subcommand(SubCommand::with_name("get").arg(key_arg).arg(uri_arg))
        .get_matches()
}

fn get_key_value(matches: &'_ ArgMatches<'_>) -> u64 {
    matches
        .value_of("key")
        .expect("key arg is required")
        .to_string()
        .parse()
        .expect("key must be u64")
}

fn get_key(k: u64) -> Vec<u8> {
    // TODO move to static
    let key_size = option_env!("BOB_KEY_SIZE")
        .map_or(Ok(8), str::parse)
        .expect("Could not parse BOB_KEY_SIZE");
    let data = k.to_le_bytes();
    (0_u8..(key_size - data.len()) as u8)
        .chain(data.iter().take(key_size).cloned())
        .collect()
}
