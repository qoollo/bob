       use bob::{Blob, BlobKey, BlobMeta, BobApiClient, GetRequest, PutRequest};
use clap::{App, Arg, ArgMatches};
use http::Uri;
use lazy_static::lazy_static;
use rand::distributions::Uniform;
use rand::prelude::*;
use std::collections::BTreeMap;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use stopwatch::Stopwatch;
use tonic::{transport::Channel, Request};

const URI_ARG_NAME: &str = "uri";
const MAX_ID_ARG_NAME: &str = "id";
const MAX_SIZE_ARG_NAME: &str = "size";
const COUNT_ARG_NAME: &str = "key";

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
            client: Client::new(settings.uri.clone()).await,
            rng: rand::thread_rng(),
            id_distribution: Uniform::new(0, settings.max_id),
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
                        log::warn!("Get size error: expected {} != actual {}", expected.0, size);
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
        let block_timestamp = self.storage.get(&key).map(|x| x.1);
        if block_timestamp.is_some() {
            return true;
        }
        let res = self.client.put(key, size, block_timestamp).await;
        match res {
            Ok(timestamp) => {
                if let Some(block_timestamp) = block_timestamp {
                    if timestamp == block_timestamp {
                        return true;
                    }
                }
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
                    if expected.0 == size {
                        true
                    } else {
                        log::warn!(
                            "Deletion size error: expected {} != actual {}",
                            expected.0,
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
    put_time: Duration,
    put_count: u64,
    get_count: u64,
    get_time: Duration,
    delete_count: u64,
    delete_time: Duration,
}

impl Client {
    async fn new(addr: Uri) -> Self {
        Self {
            client: BobApiClient::connect(addr).await.unwrap(),
            http_client: reqwest::ClientBuilder::default().build().unwrap(),
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

    async fn put(
        &mut self,
        key: u64,
        size: usize,
        block_timestamp: Option<u64>,
    ) -> Result<u64, String> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("msg: &str")
            .as_secs();
        if let Some(block_timestamp) = block_timestamp {
            if timestamp == block_timestamp {
                return Ok(timestamp);
            }
        }
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

        let sw = Stopwatch::new();
        let res = self
            .client
            .put(put_req)
            .await
            .map(|_| timestamp)
            .map_err(|e| e.to_string());
        log::debug!(
            "Put[{}] {} with size {} result: {:?}",
            timestamp,
            key,
            size,
            res
        );
        self.put_time += sw.elapsed();
        self.put_count += 1;
        res
    }

    async fn get(&mut self, key: u64) -> Result<usize, String> {
        let message = GetRequest {
            key: Some(BlobKey { key }),
            options: None,
        };
        let get_req = Request::new(message);
        let sw = Stopwatch::new();
        let res = self.client.get(get_req).await;
        let meta = if let Ok(r) = &res {
            r.get_ref().meta.clone()
        } else {
            Default::default()
        };
        let res = res
            .map(|r| r.get_ref().data.len())
            .map_err(|e| e.to_string());
        log::debug!("Get[{:?}] {} result: {:?}", meta, key, res);
        self.get_time += sw.elapsed();
        self.get_count += 1;
        res
    }

    async fn delete(&mut self, key: u64) -> Result<usize, String> {
        let message = GetRequest {
            key: Some(BlobKey { key }),
            options: None,
        };
        let get_req = Request::new(message.clone());
        let size = self
            .client
            .get(get_req)
            .await
            .map(|r| r.get_ref().data.len())
            .map_err(|e| e.to_string())?;
        log::debug!("Delete {} with size {}", key, size);
        let req = self
            .http_client
            .delete(format!("http://127.0.0.1:8001/data/{}", key))
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
        let get_req = Request::new(message);
        let res = self
            .client
            .get(get_req)
            .await
            .map(|r| r.get_ref().data.len());
        match res {
            Ok(new_size) => Err(format!("{} => {}", size, new_size)),
            Err(_) => Ok(size),
        }
    }
}

#[derive(Debug)]
struct Settings {
    count: u64,
    max_id: u64,
    max_size: usize,
    uri: Uri,
}

impl Settings {
    fn new() -> Self {
        let matches = get_matches();
        Self {
            count: Self::get_count(&matches),
            uri: Self::get_uri(&matches),
            max_id: Self::get_max_id(&matches),
            max_size: Self::get_max_size(&matches),
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

    fn get_max_id(matches: &ArgMatches) -> u64 {
        matches
            .value_of(MAX_ID_ARG_NAME)
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
    let size_arg = Arg::with_name(MAX_ID_ARG_NAME)
        .short("i")
        .long("max-id")
        .takes_value(true)
        .default_value("100000");
    let id_arg = Arg::with_name(MAX_SIZE_ARG_NAME)
        .short("s")
        .long("max-size")
        .takes_value(true)
        .default_value("100000");
    App::new("bobc")
        .arg(count_arg)
        .arg(uri_arg)
        .arg(size_arg)
        .arg(id_arg)
        .get_matches()
}
 
