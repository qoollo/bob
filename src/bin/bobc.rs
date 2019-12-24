#[macro_use]
extern crate log;

use bob::grpc::bob_api_client::BobApiClient;
use bob::grpc::{Blob, BlobKey, BlobMeta, GetRequest, PutRequest};
use clap::{App, Arg, ArgMatches, SubCommand};
use log::LevelFilter;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::Request;

#[tokio::main]
async fn main() {
    env_logger::builder().filter_level(LevelFilter::Info).init();
    let matches = build_app();
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
    let mut client = BobApiClient::connect("http://localhost:20000")
        .await
        .unwrap();

    let put_req = Request::new(PutRequest {
        key: Some(BlobKey { key }),
        data: Some(Blob {
            data: vec![1; size],
            meta: Some(BlobMeta {
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("msg: &str")
                    .as_secs() as i64,
            }),
        }),
        options: None,
    });

    let res = client.put(put_req).await;
    info!("{:#?}", res);
}

async fn get(key: u64) {
    let mut client = BobApiClient::connect("http://localhost:20000")
        .await
        .unwrap();

    let get_req = Request::new(GetRequest {
        key: Some(BlobKey { key }),
        options: None,
    });
    let res = client.get(get_req).await;
    info!("{:?}", res);
}

fn build_app<'a>() -> ArgMatches<'a> {
    let key_arg = Arg::with_name("key").takes_value(true).required(true);
    App::new("bobc")
        .subcommand(
            SubCommand::with_name("put").arg(&key_arg).arg(
                Arg::with_name("size")
                    .takes_value(true)
                    .default_value("90000"),
            ),
        )
        .subcommand(SubCommand::with_name("get").arg(key_arg))
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
