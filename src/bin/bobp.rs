use bob::grpc::{
    bob_api_client::BobApiClient, ExistRequest, GetOptions, GetRequest, GetSource, PutOptions,
    PutRequest,
};
use bob::grpc::{Blob, BlobKey, BlobMeta};
use clap::{App, Arg, ArgMatches};
use std::fmt::{Debug, Error, Formatter};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::{self, Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::delay_for;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

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
        BobApiClient::connect(endpoint).await.unwrap()
    }
}

impl Debug for NetConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{}:{}", self.target, self.port)
    }
}

#[derive(Clone, Copy)]
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
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
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

#[derive(Default)]
struct Statistics {
    put_total: AtomicU64,
    put_error: AtomicU64,

    get_total: AtomicU64,
    get_error: AtomicU64,

    put_time_ns_single_thread: AtomicU64,
    put_count_single_thread: AtomicU64,

    get_time_ns_single_thread: AtomicU64,
    get_count_single_thread: AtomicU64,

    unverified_puts: AtomicU64,
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
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(
            f,
            "workers count: {}, time: {}, behaviour: {:?}",
            self.workers_count,
            self.time
                .map(|t| format!("{:?}", t))
                .unwrap_or("infinite".to_string()),
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
    stat_thread.join().unwrap();
}

fn stat_worker(
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

        let put_error = stat.put_error.load(Ordering::Relaxed);
        let get_error = stat.get_error.load(Ordering::Relaxed);
        let put_spd = put_count_spd as f64 * k;
        let get_spd = get_count_spd as f64 * k;
        put_speed_values.push(put_spd);
        get_speed_values.push(get_spd);
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
        put resp time, ms: {:>6.2} | get resp time, ms: {:>6.2}\r\n\
        unverified put threads: {}",
        ((stat.put_total.load(Ordering::Relaxed) + stat.get_total.load(Ordering::Relaxed)) * 1000)
            .checked_div(elapsed.as_millis() as u64)
            .unwrap_or_default(),
        stat.put_error.load(Ordering::Relaxed) + stat.get_error.load(Ordering::Relaxed),
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
        stat.unverified_puts.load(Ordering::Relaxed),
    );
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
    println!("start get worker");
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
        if res.is_err() {
            stat.get_error.fetch_add(1, Ordering::SeqCst);
        }
        stat.get_total.fetch_add(1, Ordering::SeqCst);
    }
}

async fn put_worker(net_conf: NetConfig, task_conf: TaskConfig, stat: Arc<Statistics>) {
    println!("start put worker");
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
        if res.is_err() {
            stat.put_error.fetch_add(1, Ordering::SeqCst);
        }
        stat.put_total.fetch_add(1, Ordering::SeqCst);
    }
    delay_for(Duration::from_secs(1)).await;
    let keys = (task_conf.low_idx..upper_idx)
        .map(|i| BlobKey { key: i })
        .collect();
    let req = Request::new(ExistRequest {
        keys,
        options: task_conf.find_get_options(),
    });
    println!("start check exist");
    let res = client.exist(req).await;
    println!("finish check exist");
    if res.is_err() || res.unwrap().into_inner().exist.iter().any(|&b| !b) {
        stat.unverified_puts.fetch_add(1, Ordering::SeqCst);
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
        if put_res.is_err() {
            stat.put_error.fetch_add(1, Ordering::SeqCst);
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
        if get_res.is_err() {
            stat.get_error.fetch_add(1, Ordering::SeqCst);
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
    thread::spawn(move || {
        stat_worker(stop_token, 1000, stat, bytes_amount);
    })
}

fn create_blob(task_conf: &TaskConfig) -> Blob {
    let meta = BlobMeta {
        timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("msg: &str")
            .as_secs() as i64,
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
