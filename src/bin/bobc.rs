#[macro_use]
extern crate log;

use bob::grpc::bob_api_client::BobApiClient;
use bob::grpc::{Blob, BlobKey, BlobMeta, GetRequest, PutRequest};
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
                let size = sub_mathes
                    .value_of("size")
                    .unwrap()
                    .parse()
                    .expect("size must be usize");
                info!("PUT key: \"{}\" size: \"{}\"", key, size);
                put(key, size).await;
            }
            "get" => {
                info!("GET key:\"{}\" command", key);
                get(key).await;
            }
            _ => {}
        }
    }
}

async fn put(key: u64, size: usize) {
    let addr: Uri = get_matches()
        .value_of("uri")
        .expect("has default value")
        .parse()
        .expect("wrong format of url");
    let mut client = BobApiClient::connect(addr).await.unwrap();

    let put_req = Request::new(PutRequest {
        key: Some(BlobKey { key }),
        data: Some(Blob {
            data: vec![1; size],
            meta: Some(BlobMeta {
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("msg: &str")
                    .as_secs(),
            }),
        }),
        options: None,
    });

    let res = client.put(put_req).await;
    info!("{:#?}", res);
}

async fn get(key: u64) {
    let addr: Uri = get_matches()
        .value_of("uri")
        .expect("has default value")
        .parse()
        .expect("wrong format of url");
    let mut client = BobApiClient::connect(addr).await.unwrap();

    let get_req = Request::new(GetRequest {
        key: Some(BlobKey { key }),
        options: None,
    });
    let res = client.get(get_req).await;
    match res {
        Ok(res) => {
            let res: tonic::Response<_> = res;
            let data: &bob::grpc::Blob = res.get_ref();
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
    App::new("bobc")
        .subcommand(
            SubCommand::with_name("put")
                .arg(&key_arg)
                .arg(
                    Arg::with_name("size")
                        .takes_value(true)
                        .default_value("90000"),
                )
                .arg(&uri_arg),
        )
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
