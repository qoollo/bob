use bob::grpc::{
    bob_api_client::BobApiClient, GetOptions, GetRequest, GetSource, PutOptions, PutRequest,
};
use bob::grpc::{Blob, BlobKey, BlobMeta};
use clap::{App, Arg, ArgMatches};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{self, Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::delay_for;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Status};

#[macro_use]
extern crate log;

#[derive(Clone)]
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

    async fn build_client(&self) -> BobApiClient<Channel> {
        let endpoint = Endpoint::from(self.get_uri()).tcp_nodelay(true);
        loop {
            match BobApiClient::connect(endpoint.clone()).await {
                Ok(client) => return client,
                Err(e) => {
                    delay_for(Duration::from_millis(1000)).await;
                    println!(
                        "{:?}",
                        e.source()
                            .and_then(|e| e.downcast_ref::<hyper::Error>())
                            .unwrap()
                    );
                }
            }
        }
    }
}

impl Debug for NetConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}:{}", self.target, self.port)
    }
}

#[derive(Clone)]
struct TaskConfig {
    low_idx: u64,
    count: u64,
    payload_size: u64,
    direct: bool,
    measure_time: bool,
}

impl TaskConfig {
    fn from_matches(matches: &ArgMatches) -> Self {
        Self {
            low_idx: matches.value_or_default("first"),
            count: matches.value_or_default("count"),
            payload_size: matches.value_or_default("payload"),
            direct: matches.is_present("direct"),
            measure_time: false,
        }
    }

    fn find_get_options(&self) -> Option<GetOptions> {
        if self.direct {
            Some(GetOptions {
                force_node: true,
                source: GetSource::Normal as i32,
            })
        } else {
            None
        }
    }

    fn find_put_options(&self) -> Option<PutOptions> {
        if self.direct {
            Some(PutOptions {
                remote_nodes: vec![],
                force_node: true,
                overwrite: false,
            })
        } else {
            None
        }
    }

    fn is_time_measurement_thread(&self) -> bool {
        self.measure_time
    }
}

impl Debug for TaskConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "payload size: {}, count: {}",
            self.payload_size, self.count
        )?;
        if self.direct {
            write!(f, ", direct")
        } else {
            Ok(())
        }
    }
}

type CodeRepresentation = i32; // Because tonic::Code does not implement hash

#[derive(Default)]
struct Statistics {
    put_total: AtomicU64,
    put_error_count: AtomicU64,

    get_total: AtomicU64,
    get_error_count: AtomicU64,

    put_time_ns_single_thread: AtomicU64,
    put_count_single_thread: AtomicU64,

    get_time_ns_single_thread: AtomicU64,
    get_count_single_thread: AtomicU64,

    get_errors: Mutex<HashMap<CodeRepresentation, u64>>,
    put_errors: Mutex<HashMap<CodeRepresentation, u64>>,
}

impl Statistics {
    fn save_single_thread_put_time(&self, duration: &Duration) {
        self.put_time_ns_single_thread
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        self.put_count_single_thread.fetch_add(1, Ordering::Relaxed);
    }

    fn save_single_thread_get_time(&self, duration: &Duration) {
        self.get_time_ns_single_thread
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        self.get_count_single_thread.fetch_add(1, Ordering::Relaxed);
    }

    async fn save_get_error(&self, status: Status) {
        let mut guard = self.get_errors.lock().await;
        guard
            .entry(status.code() as CodeRepresentation)
            .and_modify(|i| *i += 1)
            .or_insert(1);

        self.get_error_count.fetch_add(1, Ordering::SeqCst);
        debug!("{}", status.message())
    }

    async fn save_put_error(&self, status: Status) {
        let mut guard = self.put_errors.lock().await;
        guard
            .entry(status.code() as CodeRepresentation)
            .and_modify(|i| *i += 1)
            .or_insert(1);

        self.put_error_count.fetch_add(1, Ordering::SeqCst);
        debug!("{}", status.message())
    }
}

struct BenchmarkConfig {
    workers_count: u64,
    behavior: Behavior,
    statistics: Arc<Statistics>,
    time: Option<Duration>,
    request_amount_bytes: u64,
}

#[derive(Debug)]
enum Behavior {
    Put,
    Get,
    Test,
    PingPong,
}

impl FromStr for Behavior {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "get" => Ok(Behavior::Get),
            "put" => Ok(Behavior::Put),
            "test" => Ok(Behavior::Test),
            "ping_pong" => Ok(Behavior::PingPong),
            _ => Err(()),
        }
    }
}

impl BenchmarkConfig {
    fn from_matches(matches: &ArgMatches) -> Self {
        Self {
            workers_count: matches.value_or_default("threads"),
            behavior: matches
                .value_of("behavior")
                .unwrap()
                .parse()
                .expect("incorrect behavior"),
            statistics: Arc::new(Statistics::default()),
            time: matches
                .value_of("time")
                .map(|t| Duration::from_secs(t.parse().expect("error parsing time"))),
            request_amount_bytes: matches.value_or_default("payload"),
        }
    }
}

impl Debug for BenchmarkConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "workers count: {}, time: {}, behaviour: {:?}",
            self.workers_count,
            self.time
                .map(|t| format!("{:?}", t))
                .unwrap_or_else(|| "infinite".to_string()),
            self.behavior
        )
    }
}

#[tokio::main]
async fn main() {
    let matches = get_matches();

    let net_conf = NetConfig::from_matches(&matches);
    let task_conf = TaskConfig::from_matches(&matches);
    let benchmark_conf = BenchmarkConfig::from_matches(&matches);

    println!("Bob will be benchmarked now");
    println!(
        "target: {:?}\r\nbenchmark configuration: {:?}\r\ntotal task configuration: {:?}",
        net_conf, benchmark_conf, task_conf,
    );

    let stop_token: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));

    let stat_thread = spawn_statistics_thread(&benchmark_conf, &stop_token);

    let workers = spawn_workers(&net_conf, &task_conf, &benchmark_conf);

    if let Some(time) = benchmark_conf.time {
        delay_for(time).await;
    } else {
        for worker in workers {
            let _ = worker.await;
        }
    }
    stop_token.store(true, Ordering::Relaxed);
    if let Err(e) = stat_thread.await {
        eprintln!("error awaiting stat thread: {:?}", e);
    }
}

async fn stat_worker(
    stop_token: Arc<AtomicBool>,
    period_ms: u64,
    stat: Arc<Statistics>,
    request_bytes: u64,
) {
    let pause = time::Duration::from_millis(period_ms);
    let mut last_put_count = stat.put_total.load(Ordering::Relaxed);
    let mut last_get_count = stat.get_total.load(Ordering::Relaxed);
    let mut last_st_put_time = stat.put_time_ns_single_thread.load(Ordering::Relaxed);
    let mut last_st_put_count = stat.put_count_single_thread.load(Ordering::Relaxed);
    let mut last_st_get_time = stat.get_time_ns_single_thread.load(Ordering::Relaxed);
    let mut last_st_get_count = stat.get_count_single_thread.load(Ordering::Relaxed);
    let mut put_speed_values = vec![];
    let mut get_speed_values = vec![];
    let k = request_bytes as f64 / period_ms as f64 * 1000.0 / 1024.0;
    let start = Instant::now();
    while !stop_token.load(Ordering::Relaxed) {
        thread::sleep(pause);
        let put_count_spd = get_diff(&mut last_put_count, stat.put_total.load(Ordering::Relaxed))
            * 1000
            / period_ms;
        let get_count_spd = get_diff(&mut last_get_count, stat.get_total.load(Ordering::Relaxed))
            * 1000
            / period_ms;
        let cur_st_put_time = get_diff(
            &mut last_st_put_time,
            stat.put_time_ns_single_thread.load(Ordering::Relaxed),
        ) as f64;
        let cur_st_put_count = get_diff(
            &mut last_st_put_count,
            stat.put_count_single_thread.load(Ordering::Relaxed),
        ) as f64;
        let cur_st_get_time = get_diff(
            &mut last_st_get_time,
            stat.get_time_ns_single_thread.load(Ordering::Relaxed),
        ) as f64;
        let cur_st_get_count = get_diff(
            &mut last_st_get_count,
            stat.get_count_single_thread.load(Ordering::Relaxed),
        ) as f64;

        let put_error = stat.put_error_count.load(Ordering::Relaxed);
        let get_error = stat.get_error_count.load(Ordering::Relaxed);
        let put_spd = put_count_spd as f64 * k;
        let get_spd = get_count_spd as f64 * k;
        if put_spd > 0.0 {
            put_speed_values.push(put_spd);
        }
        if get_spd > 0.0 {
            get_speed_values.push(get_spd);
        }
        println!(
            "put: {:>6} rps  | get {:>6} rps   | put err: {:5}     | get err: {:5}\r\n\
            put: {:>6.2} kb/s | get: {:>6.2} kb/s | put lat: {:>6.2} ms | get lat: {:>6.2} ms",
            put_count_spd,
            get_count_spd,
            put_error,
            get_error,
            put_spd,
            get_spd,
            finite_or_default(cur_st_put_time / cur_st_put_count / 1e9),
            finite_or_default(cur_st_get_time / cur_st_get_count / 1e9)
        );
    }
    let elapsed = start.elapsed();
    println!("Total statistics, elapsed: {:?}", elapsed);
    println!(
        "avg total: {} rps | total err: {}\r\n\
        put: {:>6.2} kb/s | get: {:>6.2} kb/s\r\n\
        put resp time, ms: {:>6.2} | get resp time, ms: {:>6.2}",
        ((stat.put_total.load(Ordering::Relaxed) + stat.get_total.load(Ordering::Relaxed)) * 1000)
            .checked_div(elapsed.as_millis() as u64)
            .unwrap_or_default(),
        stat.put_error_count.load(Ordering::Relaxed) + stat.get_error_count.load(Ordering::Relaxed),
        average(&put_speed_values),
        average(&get_speed_values),
        finite_or_default(
            (stat.put_time_ns_single_thread.load(Ordering::Relaxed) as f64)
                / (stat.put_count_single_thread.load(Ordering::Relaxed) as f64)
                / 1e9
        ),
        finite_or_default(
            (stat.get_time_ns_single_thread.load(Ordering::Relaxed) as f64)
                / (stat.get_count_single_thread.load(Ordering::Relaxed) as f64)
                / 1e9
        ),
    );
    let guard = stat.get_errors.lock().await;
    if !guard.is_empty() {
        println!("get errors:");
        for (&code, count) in guard.iter() {
            println!("{:?} = {}", Code::from(code), count);
        }
    }
    let guard = stat.put_errors.lock().await;
    if !guard.is_empty() {
        println!("put errors:");
        for (&code, count) in guard.iter() {
            println!("{:?} = {}", Code::from(code), count);
        }
    }
}

fn average(values: &[f64]) -> f64 {
    finite_or_default(values.iter().sum::<f64>() / values.len() as f64)
}

fn get_diff(last: &mut u64, current: u64) -> u64 {
    let diff = current - *last;
    *last = current;
    diff
}

fn finite_or_default(value: f64) -> f64 {
    if value.is_finite() {
        value
    } else {
        f64::default()
    }
}

async fn get_worker(net_conf: NetConfig, task_conf: TaskConfig, stat: Arc<Statistics>) {
    let mut client = net_conf.build_client().await;

    let options = task_conf.find_get_options();
    let upper_idx = task_conf.low_idx + task_conf.count;
    let measure_time = task_conf.is_time_measurement_thread();
    for i in task_conf.low_idx..upper_idx {
        let request = Request::new(GetRequest {
            key: Some(BlobKey { key: i }),
            options: options.clone(),
        });
        let res = if measure_time {
            let start = Instant::now();
            let res = client.get(request).await;
            stat.save_single_thread_get_time(&start.elapsed());
            res
        } else {
            client.get(request).await
        };
        if let Err(status) = res {
            stat.save_get_error(status).await;
        }
        stat.get_total.fetch_add(1, Ordering::SeqCst);
    }
}

async fn put_worker(net_conf: NetConfig, task_conf: TaskConfig, stat: Arc<Statistics>) {
    let mut client = net_conf.build_client().await;

    let options: Option<PutOptions> = task_conf.find_put_options();
    let measure_time = task_conf.is_time_measurement_thread();
    let upper_idx = task_conf.low_idx + task_conf.count;
    for i in task_conf.low_idx..upper_idx {
        let blob = create_blob(&task_conf);
        let key = BlobKey { key: i };
        let req = Request::new(PutRequest {
            key: Some(key),
            data: Some(blob),
            options: options.clone(),
        });
        let res = if measure_time {
            let start = Instant::now();
            let res = client.put(req).await;
            stat.save_single_thread_put_time(&start.elapsed());
            res
        } else {
            client.put(req).await
        };
        if let Err(status) = res {
            stat.save_put_error(status).await;
        }
        stat.put_total.fetch_add(1, Ordering::SeqCst);
    }
}

async fn test_worker(net_conf: NetConfig, task_conf: TaskConfig, stat: Arc<Statistics>) {
    put_worker(net_conf.clone(), task_conf.clone(), stat.clone()).await;
    get_worker(net_conf, task_conf, stat).await;
}

async fn ping_pong_worker(net_conf: NetConfig, task_conf: TaskConfig, stat: Arc<Statistics>) {
    let mut client = net_conf.build_client().await;

    let get_options = task_conf.find_get_options();
    let put_options = task_conf.find_put_options();
    let measure_time = task_conf.is_time_measurement_thread();
    let upper_idx = task_conf.low_idx + task_conf.count;
    for i in task_conf.low_idx..upper_idx {
        let blob = create_blob(&task_conf);
        let key = BlobKey { key: i };
        let put_request = Request::new(PutRequest {
            key: Some(key.clone()),
            data: Some(blob),
            options: put_options.clone(),
        });
        let put_res = if measure_time {
            let start = Instant::now();
            let res = client.put(put_request).await;
            stat.save_single_thread_put_time(&start.elapsed());
            res
        } else {
            client.put(put_request).await
        };
        if let Err(status) = put_res {
            stat.save_put_error(status).await;
        }
        stat.put_total.fetch_add(1, Ordering::SeqCst);
        let get_request = Request::new(GetRequest {
            key: Some(key),
            options: get_options.clone(),
        });
        let get_res = if measure_time {
            let start = Instant::now();
            let res = client.get(get_request).await;
            stat.save_single_thread_get_time(&start.elapsed());
            res
        } else {
            client.get(get_request).await
        };
        if let Err(status) = get_res {
            stat.save_get_error(status).await;
        }
        stat.get_total.fetch_add(1, Ordering::SeqCst);
    }
}

fn spawn_workers(
    net_conf: &NetConfig,
    task_conf: &TaskConfig,
    benchmark_conf: &BenchmarkConfig,
) -> Vec<tokio::task::JoinHandle<()>> {
    let task_size = task_conf.count / benchmark_conf.workers_count;
    (0..benchmark_conf.workers_count)
        .map(|i| {
            let nc = net_conf.clone();
            let stat_inner = benchmark_conf.statistics.clone();
            let tc = TaskConfig {
                low_idx: task_conf.low_idx + task_size * i,
                count: task_size,
                payload_size: task_conf.payload_size,
                direct: task_conf.direct,
                measure_time: i == 0,
            };
            match benchmark_conf.behavior {
                Behavior::Put => tokio::spawn(put_worker(nc, tc, stat_inner)),
                Behavior::Get => tokio::spawn(get_worker(nc, tc, stat_inner)),
                Behavior::Test => tokio::spawn(test_worker(nc, tc, stat_inner)),
                Behavior::PingPong => tokio::spawn(ping_pong_worker(nc, tc, stat_inner)),
            }
        })
        .collect()
}

fn spawn_statistics_thread(
    benchmark_conf: &BenchmarkConfig,
    stop_token: &Arc<AtomicBool>,
) -> JoinHandle<()> {
    let stop_token = stop_token.clone();
    let stat = benchmark_conf.statistics.clone();
    let bytes_amount = benchmark_conf.request_amount_bytes;
    tokio::spawn(stat_worker(stop_token, 1000, stat, bytes_amount))
}

fn create_blob(task_conf: &TaskConfig) -> Blob {
    let meta = BlobMeta {
        timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("msg: &str")
            .as_secs(),
    };
    Blob {
        data: vec![0_u8; task_conf.payload_size as usize],
        meta: Some(meta),
    }
}

fn get_matches() -> ArgMatches<'static> {
    App::new("Bob benchmark tool")
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
                .help("payload size in bytes")
                .takes_value(true)
                .short("l")
                .long("payload")
                .default_value("1024"),
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
                .help("put / get / test")
                .takes_value(true)
                .short("b")
                .long("behavior")
                .default_value("test"),
        )
        .arg(
            Arg::with_name("direct")
                .help("direct command to node")
                .short("d")
                .long("direct"),
        )
        .arg(
            Arg::with_name("time")
                .help("max time for benchmark")
                .takes_value(true)
                .long("time"),
        )
        .arg(
            Arg::with_name("amount")
                .help("amount of bytes to write")
                .takes_value(true)
                .long("amount"),
        )
        .get_matches()
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
