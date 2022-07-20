use bob::{Blob, BlobKey, BlobMeta, BobApiClient, GetRequest, PutRequest};
use clap::{App, Arg, ArgMatches};
use http::{Uri, StatusCode};
use lazy_static::lazy_static;
use rand::distributions::Uniform;
use rand::prelude::*;
use reqwest::Url;
use std::collections::BTreeMap;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use stopwatch::Stopwatch;
use tonic::{transport::Channel, Request};

const URI_ARG_NAME: &str = "uri";
const START_ID_ARG_NAME: &str = "start-id";
const END_ID_ARG_NAME: &str = "end-id";
const MAX_SIZE_ARG_NAME: &str = "size";
const COUNT_ARG_NAME: &str = "key";
const KEY_SIZE_ARG_NAME: &str = "key-size";
const API_ADDRESS_ARG_NAME: &str = "api-address";

#[tokio::main]
async fn main() {
    env_logger::init();
    let settings = Settings::new();
    let mut tester = Tester::new(settings).await;
    tester.run_test().await;
}

enum Operation {
    Get,
    Put,
    Delete,
}

impl Operation {
    fn gen<R: Rng>(rng: &mut R) -> Self {
        lazy_static! {
            static ref DIST: Uniform<u32> = Uniform::<u32>::new(0, 3);
        }
        match DIST.sample(rng) {
            0 => Self::Get,
            1 => Self::Put,
            2 => Self::Delete,
            _ => Self::Get,
        }
    }
}

struct Tester {
    storage: BTreeMap<u64, usize>,
    client: Client,
    settings: Settings,
    rng: rand::rngs::ThreadRng,
    id_distribution: rand::distributions::Uniform<u64>,
    size_distribution: rand::distributions::Uniform<usize>,
}

impl Tester {
    async fn new(settings: Settings) -> Tester {
        Self {
            storage: Default::default(),
            client: Client::new(settings.clone()).await,
            rng: rand::thread_rng(),
            id_distribution: Uniform::new(settings.start_id, settings.end_id),
            size_distribution: Uniform::new(0, settings.max_size),
            settings,
        }
    }

    fn rand_id(&mut self) -> u64 {
        self.id_distribution.sample(&mut self.rng)
    }

    fn rand_size(&mut self) -> usize {
        self.size_distribution.sample(&mut self.rng)
    }

    async fn get(&mut self) -> bool {
        let key = self.rand_id();
        let res = self.client.get(key).await;
        match res {
            Ok(size) => {
                if let Some(expected) = self.storage.get(&key) {
                    if *expected == size {
                        true
                    } else {
                        log::warn!("Get size error: expected {} != actual {}", expected, size);
                        false
                    }
                } else {
                    log::warn!("Get unexpected of size {}", size);
                    false
                }
            }
            Err(e) => {
                if self.storage.contains_key(&key) {
                    log::warn!("Get unexpected error: {}", e);
                    false
                } else {
                    true
                }
            }
        }
    }

    async fn put(&mut self) -> bool {
        let key = self.rand_id();
        let size = self.rand_size();
        let res = self.client.put(key, size).await;
        match res {
            Ok(_) => {
                self.storage.insert(key, size);
                true
            }
            Err(e) => {
                log::warn!("Put error: {}", e);
                false
            }
        }
    }

    async fn delete(&mut self) -> bool {
        let key = self.rand_id();
        let res = self.client.delete(key).await;
        let value = self.storage.remove(&key);
        match res {
            Ok(size) => {
                if let Some(expected) = value {
                    if expected == size {
                        true
                    } else {
                        log::warn!(
                            "Deletion size error: expected {} != actual {}",
                            expected,
                            size
                        );
                        false
                    }
                } else {
                    log::warn!("Unexpected deletion of size {}", size);
                    false
                }
            }
            Err(e) => {
                if value.is_none() {
                    true
                } else {
                    log::warn!("Unexpected deletion error: {}", e);
                    false
                }
            }
        }
    }

    async fn run_test(&mut self) {
        let mut total_succ: u64 = 0;
        for i in 0..self.settings.count {
            if i % 10000 == 0 {
                log::info!("Summary: {}/{}", total_succ, i);
                self.client.print_summary();
                self.client.reset_metrics();
            }
            let op = Operation::gen(&mut self.rng);
            let success = match op {
                Operation::Get => self.get().await,
                Operation::Put => self.put().await,
                Operation::Delete => self.delete().await,
            };
            if success {
                total_succ += 1;
            }
        }
    }
}

struct Client {
    client: BobApiClient<Channel>,
    http_client: reqwest::Client,
    settings: Settings,
    put_time: Duration,
    put_count: u64,
    get_count: u64,
    get_time: Duration,
    delete_count: u64,
    delete_time: Duration,
}

impl Client {
    async fn new(settings: Settings) -> Self {
        Self {
            client: BobApiClient::connect(settings.uri.clone()).await.unwrap(),
            http_client: reqwest::ClientBuilder::default().build().unwrap(),
            settings,
            put_count: 0,
            put_time: Duration::ZERO,
            get_count: 0,
            get_time: Duration::ZERO,
            delete_count: 0,
            delete_time: Duration::ZERO,
        }
    }

    fn reset_metrics(&mut self) {
        self.put_time = Duration::ZERO;
        self.put_count = 0;
        self.get_time = Duration::ZERO;
        self.get_count = 0;
        self.delete_time = Duration::ZERO;
        self.delete_count = 0;
    }

    fn print_summary(&self) {
        let latency = self.get_time.as_secs_f64() / self.get_count as f64 * 1000.0;
        log::info!("GET: count: {}, latency: {}", self.get_count, latency);
        let latency = self.put_time.as_secs_f64() / self.put_count as f64 * 1000.0;
        log::info!("PUT: count: {}, latency: {}", self.put_count, latency);
        let latency = self.delete_time.as_secs_f64() / self.delete_count as f64 * 1000.0;
        log::info!("DELETE: count: {}, latency: {}", self.delete_count, latency);
    }

    async fn put(&mut self, key: u64, size: usize) -> Result<(), String> {
        let data = vec![1; size];
        let mut url = Url::parse(&self.settings.api_uri.to_string()).unwrap();
        url.set_path(&format!("/data/{}", self.settings.convert_key(key)));
        let addr = url.to_string();
        log::info!("Put to {}", addr);
        let sw = Stopwatch::new();
        let req = self.http_client
            .post(addr)
            .body(data)
            .header("username", "admin")
            .header("password", "password")
            .build()
            .map_err(|e| e.to_string())?;
        let res = self.http_client.execute(req).await.map_err(|e| e.to_string())?;
        if !res.status().is_success() {
            log::warn!("Put resulted in error code: {:?}", res.status());
        }
        log::info!(
            "Put {} with size {}",
            key,
            size,
        );
        self.put_time += sw.elapsed();
        self.put_count += 1;
        Ok(())
    }

    async fn get(&mut self, key: u64) -> Result<usize, String> {
        let mut url = Url::parse(&self.settings.api_uri.to_string()).unwrap();
        url.set_path(&format!("/data/{}", self.settings.convert_key(key)));
        let addr = url.to_string();//format!("{}/data/{}", self.settings.api_uri, self.settings.convert_key(key));
        let req = self.http_client
            .get(addr)
            .header("username", "admin")
            .header("password", "password")
            .build()
            .map_err(|e| e.to_string())?;
        let sw = Stopwatch::new();
        let res = self.http_client.execute(req).await.map_err(|e| e.to_string())?;
        let status = res.status();
        if !status.is_success() {
            if status != StatusCode::NOT_FOUND {
                log::warn!("Get resulted in error code: {:?}", res.status());
            }
            return Err(status.to_string());
        }
        self.get_time += sw.elapsed();
        self.get_count += 1;
        log::debug!("Get {} result: {:?}", key, res);
        let bytes = res.bytes().await;
        bytes.map(|b| b.len()).map_err(|e| e.to_string())
    }

    async fn delete(&mut self, key: u64) -> Result<usize, String> {
        let size = self.get(key).await?;
        log::debug!("Delete {} with size {}", key, size);
        let req = self
            .http_client
            .delete(format!("{}/data/{}", self.settings.api_uri, self.settings.convert_key(key)))
            .build()
            .map_err(|e| e.to_string())?;
        let sw = Stopwatch::new();
        let _ = self
            .http_client
            .execute(req)
            .await
            .map_err(|e| e.to_string());
        self.delete_time += sw.elapsed();
        self.delete_count += 1;
        let res = self.get(key).await;
        match res {
            Ok(new_size) => Err(format!("{} => {}", size, new_size)),
            Err(_) => Ok(size),
        }
    }
}

#[derive(Debug, Clone)]
struct Settings {
    count: u64,
    start_id: u64,
    end_id: u64,
    max_size: usize,
    key_size: usize,
    uri: Uri,
    api_uri: Uri,
}

impl Settings {
    fn new() -> Self {
        let matches = get_matches();
        Self {
            count: Self::get_count(&matches),
            uri: Self::get_uri(&matches),
            start_id: Self::get_start_id(&matches),
            end_id: Self::get_end_id(&matches),
            max_size: Self::get_max_size(&matches),
            key_size: Self::get_key_size(&matches),
            api_uri: Self::get_api_uri(&matches),
        }
    }

    fn get_count(matches: &ArgMatches) -> u64 {
        matches
            .value_of(COUNT_ARG_NAME)
            .expect("required")
            .parse()
            .expect("should be u64")
    }

    fn get_max_size(matches: &ArgMatches) -> usize {
        matches
            .value_of(MAX_SIZE_ARG_NAME)
            .expect("has default")
            .parse()
            .expect("should be usize")
    }

    fn get_start_id(matches: &ArgMatches) -> u64 {
        matches
            .value_of(START_ID_ARG_NAME)
            .expect("has default")
            .parse()
            .expect("should be u64")
    }

    fn get_end_id(matches: &ArgMatches) -> u64 {
        matches
            .value_of(END_ID_ARG_NAME)
            .expect("has default")
            .parse()
            .expect("should be u64")
    }

    fn get_uri(matches: &ArgMatches) -> Uri {
        matches
            .value_of(URI_ARG_NAME)
            .expect("has default value")
            .parse()
            .expect("wrong format of url")
    }

    fn get_key_size(matches: &ArgMatches) -> usize {
        matches
            .value_of(KEY_SIZE_ARG_NAME)
            .expect("has default value")
            .parse()
            .expect("wrong format of url")
    }

    fn get_api_uri(matches: &ArgMatches) -> Uri {
        matches
            .value_of(API_ADDRESS_ARG_NAME)
            .expect("has default value")
            .parse()
            .expect("wrong format of url")
    }

    fn convert_key(&self, key: u64) -> String {
        let mut data = key.to_le_bytes().to_vec();
        data.resize(self.key_size, 0);
        let mut result = String::with_capacity(data.len() * 2 + 2);
        result.push_str("0x");
        for b in data {
            result.push_str(&format!("{:X?}", b));
        }
        result
    }
}

fn get_matches<'a>() -> ArgMatches<'a> {
    let count_arg = Arg::with_name(COUNT_ARG_NAME)
        .short("c")
        .long("count")
        .takes_value(true)
        .required(true);
    let uri_arg = Arg::with_name(URI_ARG_NAME)
        .short("a")
        .long("address")
        .takes_value(true)
        .default_value("http://localhost:20000");
    let start_id_arg = Arg::with_name(START_ID_ARG_NAME)
        .long("start-id")
        .takes_value(true)
        .default_value("0");
    let end_id_arg = Arg::with_name(END_ID_ARG_NAME)
        .long("end-id")
        .takes_value(true)
        .default_value("100000");
    let size_arg = Arg::with_name(MAX_SIZE_ARG_NAME)
        .short("s")
        .long("max-size")
        .takes_value(true)
        .default_value("100000");
    let key_size_arg = Arg::with_name(KEY_SIZE_ARG_NAME)
        .long("key-size")
        .takes_value(true)
        .default_value("8");
    let api_uri_arg = Arg::with_name(API_ADDRESS_ARG_NAME)
        .long("api-address")
        .takes_value(true)
        .default_value("http://localhost:8000");
    App::new("bobt")
        .arg(count_arg)
        .arg(uri_arg)
        .arg(size_arg)
        .arg(start_id_arg)
        .arg(end_id_arg)
        .arg(key_size_arg)
        .arg(api_uri_arg)
        .get_matches()
}
