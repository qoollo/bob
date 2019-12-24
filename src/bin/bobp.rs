#[macro_use]
extern crate log;

use bob::grpc::{
    bob_api_client::BobApiClient, GetOptions, GetRequest, GetSource, PutOptions, PutRequest,
};
use bob::grpc::{Blob, BlobKey, BlobMeta};
use clap::{App, Arg};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{self, Duration, SystemTime, UNIX_EPOCH};
use tokio::time::delay_for;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

async fn build_client(net_conf: NetConfig) -> BobApiClient<Channel> {
    let endpoint = Endpoint::from(net_conf.get_uri()).tcp_nodelay(true);
    BobApiClient::connect(endpoint).await.unwrap()
}

#[derive(Debug, Clone)]
struct NetConfig {
    port: u16,
    target: String,
}

impl NetConfig {
    pub fn get_uri(&self) -> http::Uri {
        http::Uri::builder()
            .scheme("http")
            .authority(format!("{}:{}", self.target, self.port).as_str())
            .path_and_query("/")
            .build()
            .unwrap()
    }
}

#[derive(Debug, Clone, Copy)]
struct TaskConfig {
    low_idx: u64,
    count: u64,
    payload_size: u64,
    direct: bool,
}

struct Stat {
    put_total: AtomicU64,
    put_error: AtomicU64,

    get_total: AtomicU64,
    get_error: AtomicU64,
}

fn stat_worker(stop_token: Arc<AtomicBool>, period_ms: u64, stat: Arc<Stat>) {
    let pause = time::Duration::from_millis(period_ms);
    let mut last_put_count = stat.put_total.load(Ordering::Relaxed);
    let mut last_get_count = stat.get_total.load(Ordering::Relaxed);

    while !stop_token.load(Ordering::Relaxed) {
        thread::sleep(pause);
        let cur_put_count = stat.put_total.load(Ordering::Relaxed);
        let put_count_spd = (cur_put_count - last_put_count) * 1000 / period_ms;
        last_put_count = cur_put_count;

        let cur_get_count = stat.get_total.load(Ordering::Relaxed);
        let get_count_spd = (cur_get_count - last_get_count) * 1000 / period_ms;
        last_get_count = cur_get_count;

        let put_error = stat.put_error.load(Ordering::Relaxed);
        let get_error = stat.get_error.load(Ordering::Relaxed);
        println!(
            "put: {:5} rps | get {:5} rps | put err: {:5} | get err: {:5}",
            put_count_spd, get_count_spd, put_error, get_error
        );
    }
}

async fn get_worker(net_conf: NetConfig, task_conf: TaskConfig, stat: Arc<Stat>) {
    let mut client = build_client(net_conf).await;

    let options = if task_conf.direct {
        Some(GetOptions {
            force_node: true,
            source: GetSource::Normal as i32,
        })
    } else {
        None
    };

    let upper_idx = task_conf.low_idx + task_conf.count;
    for i in task_conf.low_idx..upper_idx {
        let res = client
            .get(Request::new(GetRequest {
                key: Some(BlobKey { key: i }),
                options: options.clone(),
            }))
            .await;
        if res.is_err() {
            stat.get_error.fetch_add(1, Ordering::SeqCst);
        }
        stat.get_total.fetch_add(1, Ordering::SeqCst);
    }
}

async fn put_worker(net_conf: NetConfig, task_conf: TaskConfig, stat: Arc<Stat>) {
    let mut client = build_client(net_conf.clone()).await;

    let options: Option<PutOptions> = if task_conf.direct {
        Some(PutOptions {
            remote_nodes: vec![],
            force_node: true,
            overwrite: false,
        })
    } else {
        None
    };

    let upper_idx = task_conf.low_idx + task_conf.count;
    for i in task_conf.low_idx..upper_idx {
        let meta = BlobMeta {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("msg: &str")
                .as_secs() as i64,
        };
        let blob = Blob {
            data: vec![0; task_conf.payload_size as usize],
            meta: Some(meta),
        };
        let key = BlobKey { key: i };
        let req = Request::new(PutRequest {
            key: Some(key),
            data: Some(blob),
            options: options.clone(),
        });
        let res = client.put(req).await;
        if res.is_err() {
            stat.put_error.fetch_add(1, Ordering::SeqCst);
        }
        stat.put_total.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::main]
async fn main() {
    let matches = App::new("Bob benchmark tool")
        .arg(
            Arg::with_name("host")
                .help("ip or hostname of bob")
                .takes_value(true)
                .short("h")
                .long("host")
                .default_value("127.0.0.1"),
        )
        .arg(
            Arg::with_name("port")
                .help("port of bob")
                .takes_value(true)
                .short("p")
                .long("port")
                .default_value("20000"),
        )
        .arg(
            Arg::with_name("payload")
                .help("payload size")
                .takes_value(true)
                .short("l")
                .long("payload")
                .default_value("100000"),
        )
        .arg(
            Arg::with_name("threads")
                .help("worker thread count")
                .takes_value(true)
                .short("t")
                .long("threads")
                .default_value("1"),
        )
        .arg(
            Arg::with_name("count")
                .help("count of records to proceed")
                .takes_value(true)
                .short("c")
                .long("count")
                .default_value("1000000"),
        )
        .arg(
            Arg::with_name("first")
                .help("first index of records to proceed")
                .takes_value(true)
                .short("f")
                .long("first")
                .default_value("0"),
        )
        .arg(
            Arg::with_name("behavior")
                .help("put / get")
                .takes_value(true)
                .short("b")
                .long("behavior")
                .default_value("put"),
        )
        .arg(
            Arg::with_name("direct")
                .help("direct command to node")
                .short("d")
                .long("direct"),
        )
        .get_matches();

    let net_conf = NetConfig {
        port: matches
            .value_of("port")
            .unwrap_or_default()
            .parse()
            .unwrap(),
        target: matches.value_of("host").unwrap_or_default().to_string(),
    };

    let task_conf = TaskConfig {
        low_idx: matches
            .value_of("first")
            .unwrap_or_default()
            .parse()
            .unwrap(),
        count: matches
            .value_of("count")
            .unwrap_or_default()
            .parse()
            .unwrap(),
        payload_size: matches
            .value_of("payload")
            .unwrap_or_default()
            .parse()
            .unwrap(),
        direct: matches.is_present("direct"),
    };

    let workers_count: u64 = matches
        .value_of("threads")
        .unwrap_or_default()
        .parse()
        .unwrap();

    let behavior = match matches.value_of("behavior").unwrap_or_default() {
        "put" => true,
        "get" => false,
        value => panic!("invalid value for behavior: {}", value),
    };

    println!("Bob will be benchmarked now");
    println!(
        "target: {}:{} workers_count: {} payload size: {} total records: {}",
        net_conf.target, net_conf.port, workers_count, task_conf.payload_size, task_conf.count,
    );

    let stat = Arc::new(Stat {
        put_total: AtomicU64::new(0),
        put_error: AtomicU64::new(0),

        get_total: AtomicU64::new(0),
        get_error: AtomicU64::new(0),
    });

    let stop_token: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
    let stat_cln = stat.clone();

    let stat_thread = {
        let stop_token = stop_token.clone();
        thread::spawn(move || {
            stat_worker(stop_token, 1000, stat_cln);
        })
    };

    let task_size = task_conf.count / workers_count;
    for i in 0..workers_count {
        let nc = net_conf.clone();
        let stat_inner = stat.clone();
        let tc = TaskConfig {
            low_idx: task_conf.low_idx + task_size * i,
            count: task_size,
            payload_size: task_conf.payload_size,
            direct: task_conf.direct,
        };
        if behavior {
            tokio::spawn(put_worker(nc, tc, stat_inner));
        } else {
            tokio::spawn(get_worker(nc, tc, stat_inner));
        }
        error!("worker spawned");
    }

    delay_for(Duration::from_secs(10)).await;
    stop_token.store(true, Ordering::Relaxed);

    stat_thread.join().unwrap();
}
