use bob::{
    Blob, BlobKey, BlobMeta, BobApiClient, ExistRequest, GetOptions, GetRequest, GetSource,
    PutOptions, PutRequest,
};

use clap::{App, Arg, ArgMatches};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use lazy_static::lazy_static;
use serde::Serialize;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::ops::Sub;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::RwLock;
use std::sync::{Arc, Mutex as StdMutex};
use std::thread;
use std::time::{self, Duration, Instant, SystemTime, UNIX_EPOCH};
use sysinfo::{DiskExt, RefreshKind, System, SystemExt};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::interval;
use tokio::time::sleep;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Status};

#[macro_use]
extern crate log;

fn pb_with_prefix(prefix: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner()
        .with_style(ProgressStyle::default_spinner().template("{prefix}: {msg}"));
    pb.set_prefix(prefix);
    pb
}

fn pb2_with_prefix(prefix: &str) -> ProgressBar {
    let style = ProgressStyle::default_bar().progress_chars("█▉▊▋▌▍▎▏  ");
    let pb = ProgressBar::new(100).with_style(style);
    pb.set_prefix(prefix);
    pb
}

lazy_static! {
    static ref MPB: MultiProgress = MultiProgress::new();
    static ref LOG_PB: ProgressBar = MPB.add(pb_with_prefix("LOG"));
    static ref MAIN_PB: ProgressBar = MPB.add(pb2_with_prefix("REPEATS"));
    static ref SECOND_PB: ProgressBar = MPB.add(pb2_with_prefix("REQUESTS"));
    static ref GET_PB: ProgressBar = MPB.add(pb_with_prefix("GET"));
    static ref PUT_PB: ProgressBar = MPB.add(pb_with_prefix("PUT"));
    static ref SYSTEM: RwLock<System> = RwLock::new(System::new_with_specifics(
        RefreshKind::new()
            .with_disks()
            .with_disks_list()
            .with_memory(),
    ));
    static ref START: Instant = Instant::now();
    static ref SAVE_LOCK: StdMutex<()> = StdMutex::new(());
}

fn finish_all_pb() {
    MAIN_PB.finish_and_clear();
    SECOND_PB.finish_and_clear();
    LOG_PB.finish_and_clear();
    GET_PB.finish_and_clear();
    PUT_PB.finish_and_clear();
}

const ORD: Ordering = Ordering::Relaxed;

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
                    sleep(Duration::from_millis(1000)).await;
                    LOG_PB.set_message(&format!(
                        "{:?}",
                        e.source()
                            .and_then(|e| e.downcast_ref::<hyper::Error>())
                            .unwrap()
                    ));
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
    time: Option<Duration>,
}

impl TaskConfig {
    fn from_matches(matches: &ArgMatches) -> Self {
        Self {
            low_idx: matches.value_or_default("first"),
            count: matches.value_or_default("count"),
            payload_size: matches.value_or_default("payload"),
            direct: matches.is_present("direct"),
            time: matches
                .value_of("time")
                .map(|t| Duration::from_secs(t.parse().expect("error parsing time"))),
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
            "payload size: {}, count: {}, time: {}",
            self.payload_size,
            self.count,
            self.time
                .map(|t| format!("{:?}", t))
                .unwrap_or_else(|| "infinite".to_string()),
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

    put_time_ns_st: AtomicU64,
    put_count_st: AtomicU64,

    get_time_ns_st: AtomicU64,
    get_count_st: AtomicU64,

    get_errors: Mutex<HashMap<CodeRepresentation, u64>>,
    put_errors: Mutex<HashMap<CodeRepresentation, u64>>,

    get_size_bytes: AtomicU64,

    verified_puts: AtomicU64,

    mode: RwLock<String>,
    iteration: AtomicU64,
}

impl Statistics {
    fn save_single_thread_put_time(&self, duration: &Duration) {
        self.put_time_ns_st
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        self.put_count_st.fetch_add(1, Ordering::Relaxed);
    }

    fn save_single_thread_get_time(&self, duration: &Duration) {
        self.get_time_ns_st
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        self.get_count_st.fetch_add(1, Ordering::Relaxed);
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

#[derive(Debug, Serialize)]
struct StatisticsToSave {
    timestamp: u128,
    put_total: u64,
    put_error_count: u64,

    get_total: u64,
    get_error_count: u64,

    put_time_ns_st: u64,
    put_count_st: u64,

    get_time_ns_st: u64,
    get_count_st: u64,

    get_size_bytes: u64,

    verified_puts: u64,

    disks: HashMap<String, (u64, u64)>,

    mode: String,

    iteration: u64,
}

impl StatisticsToSave {
    fn new(stats: &Arc<Statistics>) -> Self {
        SYSTEM.write().unwrap().refresh_all();
        let sys = SYSTEM.read().unwrap();
        Self {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            get_total: stats.get_total.load(ORD),
            get_count_st: stats.get_count_st.load(ORD),
            get_time_ns_st: stats.get_time_ns_st.load(ORD),
            get_size_bytes: stats.get_size_bytes.load(ORD),
            get_error_count: stats.get_error_count.load(ORD),
            put_count_st: stats.put_count_st.load(ORD),
            put_error_count: stats.put_error_count.load(ORD),
            put_time_ns_st: stats.put_time_ns_st.load(ORD),
            put_total: stats.put_total.load(ORD),
            verified_puts: stats.verified_puts.load(ORD),
            disks: sys
                .get_disks()
                .iter()
                .map(|disk| {
                    (
                        format!(
                            "{}:{}",
                            disk.get_name().to_str().unwrap(),
                            disk.get_mount_point().to_str().unwrap()
                        ),
                        (disk.get_available_space(), disk.get_total_space()),
                    )
                })
                .collect(),
            mode: stats.mode.read().unwrap().clone(),
            iteration: stats.iteration.load(ORD),
        }
    }
}

#[derive(Clone)]
struct BenchmarkConfig {
    workers_count: u64,
    behavior: Behavior,
    statistics: Arc<Statistics>,
    request_amount_bytes: u64,
    save_name: Option<String>,
    save_period_sec: u64,
    repeat: u64,
}

#[derive(Debug, Clone)]
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
            request_amount_bytes: matches.value_or_default("payload"),
            save_name: matches.value_of("metrics").map(|x| x.to_string()),
            repeat: matches.value_or_default("repeat"),
            save_period_sec: matches.value_or_default("save_period"),
        }
    }
}

impl Debug for BenchmarkConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "workers count: {}, behaviour: {:?}",
            self.workers_count, self.behavior
        )
    }
}

struct DiffContainer<'a, T: Copy + Sub<Output = T>> {
    last_value: T,
    value_getter: Box<dyn Fn() -> T + 'a + Send>,
}

impl<'a, T: Copy + Sub<Output = T>> DiffContainer<'a, T> {
    pub fn new(f: Box<dyn Fn() -> T + 'a + Send>) -> Self {
        DiffContainer {
            last_value: f(),
            value_getter: f,
        }
    }

    pub fn get_diff(&mut self) -> T {
        let current = (*self.value_getter)();
        let diff = current - self.last_value;
        self.last_value = current;
        diff
    }
}

#[tokio::main]
async fn main() {
    let matches = get_matches();
    let net_conf = NetConfig::from_matches(&matches);
    let task_conf = TaskConfig::from_matches(&matches);
    let benchmark_conf = BenchmarkConfig::from_matches(&matches);

    let stop_token: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
    // let _ = spawn_disks_worker(stop_token.clone()).await;

    println!("Bob will be benchmarked now");
    println!(
        "target: {:?}\nbenchmark configuration: {:?}\ntotal task configuration: {:?}",
        net_conf, benchmark_conf, task_conf,
    );

    LOG_PB.tick();

    let stat_thread = spawn_statistics_thread(&benchmark_conf, &stop_token);

    MAIN_PB.set_length(benchmark_conf.repeat);

    run_workers(&benchmark_conf, &net_conf, &task_conf).await;

    stop_token.store(true, Ordering::Relaxed);
    if let Err(e) = stat_thread.await {
        LOG_PB.println(&format!("error awaiting stat thread: {:?}", e));
    }
}

async fn run_workers(
    benchmark_conf: &BenchmarkConfig,
    net_conf: &NetConfig,
    task_conf: &TaskConfig,
) {
    let mut task_conf = task_conf.clone();
    let stats = benchmark_conf.statistics.clone();
    let save_name = benchmark_conf.save_name.as_ref();
    for _ in 0..benchmark_conf.repeat {
        match benchmark_conf.behavior {
            Behavior::Test => {
                let mut benchmark_conf = benchmark_conf.clone();
                benchmark_conf.behavior = Behavior::Put;
                *stats.mode.write().unwrap() = format!("{:?}", benchmark_conf.behavior);
                let workers = spawn_workers(&net_conf, &task_conf, &benchmark_conf);
                for worker in workers {
                    let _ = worker.await;
                }
                save_stat(&stats, save_name);
                benchmark_conf.behavior = Behavior::Get;
                *stats.mode.write().unwrap() = format!("{:?}", benchmark_conf.behavior);
                let workers = spawn_workers(&net_conf, &task_conf, &benchmark_conf);
                for worker in workers {
                    let _ = worker.await;
                }
                save_stat(&stats, save_name);
            }
            _ => {
                *stats.mode.write().unwrap() = format!("{:?}", benchmark_conf.behavior);
                let workers = spawn_workers(&net_conf, &task_conf, &benchmark_conf);
                for worker in workers {
                    let _ = worker.await;
                }
                save_stat(&stats, save_name);
            }
        }
        MAIN_PB.inc(1);
        task_conf.low_idx += task_conf.count;
        benchmark_conf.statistics.iteration.fetch_add(1, ORD);
    }
}

async fn stat_worker(
    stop_token: Arc<AtomicBool>,
    period_ms: u64,
    stat: Arc<Statistics>,
    request_bytes: u64,
    save_name: Option<String>,
    save_period_sec: u64,
    mpb_join: JoinHandle<()>,
) {
    let (put_speed_values, get_speed_values, elapsed) = print_periodic_stat(
        stop_token,
        period_ms,
        &stat,
        request_bytes,
        save_name,
        save_period_sec,
    );
    finish_all_pb();
    if let Err(err) = mpb_join.await {
        println!("Join progress bar error: {}", err);
    }
    print_averages(&stat, &put_speed_values, &get_speed_values, elapsed);
    print_errors_with_codes(stat).await
}

async fn print_errors_with_codes(stat: Arc<Statistics>) {
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

fn print_averages(
    stat: &Arc<Statistics>,
    put_speed_values: &[f64],
    get_speed_values: &[f64],
    elapsed: Duration,
) {
    let mut stat_message = format!(
        "STAT: avg total: {} rps, total err: {}",
        ((stat.put_total.load(Ordering::Relaxed) + stat.get_total.load(Ordering::Relaxed)) * 1000)
            .checked_div(elapsed.as_millis() as u64)
            .unwrap_or_default(),
        stat.put_error_count.load(Ordering::Relaxed) + stat.get_error_count.load(Ordering::Relaxed),
    );
    println!(
        "GET: speed: {} kb/s, resp time: {:>6.2} ms",
        average(&get_speed_values),
        finite_or_default(
            (stat.get_time_ns_st.load(Ordering::Relaxed) as f64)
                / (stat.get_count_st.load(Ordering::Relaxed) as f64)
                / 1e9
        ),
    );
    println!(
        "PUT: speed: {} kb/s, resp time: {:>6.2} ms",
        average(&put_speed_values),
        finite_or_default(
            (stat.put_time_ns_st.load(Ordering::Relaxed) as f64)
                / (stat.put_count_st.load(Ordering::Relaxed) as f64)
                / 1e9
        ),
    );
    if get_matches().is_present("verify") {
        stat_message = format!(
            "{}, verified put threads: {}",
            stat_message,
            stat.verified_puts.load(Ordering::Relaxed)
        );
    }
    println!("{}", &stat_message);
}

fn print_periodic_stat(
    stop_token: Arc<AtomicBool>,
    period_ms: u64,
    stat: &Arc<Statistics>,
    request_bytes: u64,
    save_name: Option<String>,
    save_period_sec: u64,
) -> (Vec<f64>, Vec<f64>, Duration) {
    let pause = time::Duration::from_millis(period_ms);
    let mut put_count = DiffContainer::new(Box::new(|| stat.put_total.load(Ordering::Relaxed)));
    let mut get_count = DiffContainer::new(Box::new(|| stat.get_total.load(Ordering::Relaxed)));
    let mut put_time_st =
        DiffContainer::new(Box::new(|| stat.put_time_ns_st.load(Ordering::Relaxed)));
    let mut put_count_st =
        DiffContainer::new(Box::new(|| stat.put_count_st.load(Ordering::Relaxed)));
    let mut get_time_st =
        DiffContainer::new(Box::new(|| stat.get_time_ns_st.load(Ordering::Relaxed)));
    let mut get_count_st =
        DiffContainer::new(Box::new(|| stat.get_count_st.load(Ordering::Relaxed)));
    let mut get_size = DiffContainer::new(Box::new(|| stat.get_size_bytes.load(Ordering::Relaxed)));
    let mut put_speed_values = vec![];
    let mut get_speed_values = vec![];
    let k = request_bytes as f64 / period_ms as f64 * 1000.0 / 1024.0;
    let start = Instant::now();
    save_stat(stat, save_name.as_ref());
    let mut last_save = Instant::now();
    while !stop_token.load(Ordering::Relaxed) {
        thread::sleep(pause);
        if last_save.elapsed() > Duration::from_secs(save_period_sec) {
            save_stat(stat, save_name.as_ref());
            last_save = Instant::now();
        }
        let put_count_spd = put_count.get_diff() * 1000 / period_ms;
        let get_count_spd = get_count.get_diff() * 1000 / period_ms;
        let cur_st_put_time = put_time_st.get_diff() as f64;
        let cur_st_put_count = put_count_st.get_diff() as f64;
        let cur_st_get_time = get_time_st.get_diff() as f64;
        let cur_st_get_count = get_count_st.get_diff() as f64;

        let put_error = stat.put_error_count.load(Ordering::Relaxed);
        let get_error = stat.get_error_count.load(Ordering::Relaxed);
        let put_spd = put_count_spd as f64 * k;
        let get_spd = get_size.get_diff() as f64 / 1024.0;
        if put_spd > 0.0 {
            put_speed_values.push(put_spd);
        }
        if get_spd > 0.0 {
            get_speed_values.push(get_spd);
        }
        GET_PB.set_message(&format!(
            "{:>7} rps, {:>7.2} kb/s, {:5} errors, {:>7.2} ms",
            get_count_spd,
            get_spd,
            get_error,
            finite_or_default(cur_st_get_time / cur_st_get_count / 1e9)
        ));
        PUT_PB.set_message(&format!(
            "{:>7} rps, {:>7.2} kb/s, {:5} errors, {:>7.2} ms",
            put_count_spd,
            put_spd,
            put_error,
            finite_or_default(cur_st_put_time / cur_st_put_count / 1e9)
        ));
    }
    save_stat(stat, save_name.as_ref());
    let elapsed = start.elapsed();
    LOG_PB.println(&format!("Total statistics, elapsed: {:?}", elapsed));
    (put_speed_values, get_speed_values, elapsed)
}

fn average(values: &[f64]) -> f64 {
    finite_or_default(values.iter().sum::<f64>() / values.len() as f64)
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
    let worker_start = Instant::now();
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
        match res {
            Err(status) => stat.save_get_error(status).await,
            Ok(payload) => {
                let res = payload.into_inner().data;
                stat.get_size_bytes
                    .fetch_add(res.len() as u64, Ordering::SeqCst);
            }
        }
        stat.get_total.fetch_add(1, Ordering::SeqCst);
        SECOND_PB.inc(1);
        if let Some(time) = &task_conf.time {
            if Instant::now().duration_since(worker_start) > *time {
                break;
            }
        }
    }
}

async fn put_worker(net_conf: NetConfig, task_conf: TaskConfig, stat: Arc<Statistics>) {
    let mut client = net_conf.build_client().await;

    let options: Option<PutOptions> = task_conf.find_put_options();
    let measure_time = task_conf.is_time_measurement_thread();
    let upper_idx = task_conf.low_idx + task_conf.count;
    let worker_start = Instant::now();
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
        SECOND_PB.inc(1);
        if let Some(time) = &task_conf.time {
            if Instant::now().duration_since(worker_start) > *time {
                break;
            }
        }
    }
    if get_matches().is_present("verify") {
        let req = Request::new(ExistRequest {
            keys: (task_conf.low_idx..upper_idx)
                .map(|i| BlobKey { key: i })
                .collect(),
            options: task_conf.find_get_options(),
        });
        let res = client.exist(req).await;
        if let Ok(res) = res {
            if res.into_inner().exist.iter().all(|b| *b) {
                stat.verified_puts.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

async fn test_worker(
    net_conf: NetConfig,
    task_conf: TaskConfig,
    stat: Arc<Statistics>,
    save_name: Option<String>,
) {
    put_worker(net_conf.clone(), task_conf.clone(), stat.clone()).await;
    save_stat(&stat, save_name.as_ref());
    get_worker(net_conf, task_conf, stat).await;
}

async fn ping_pong_worker(net_conf: NetConfig, task_conf: TaskConfig, stat: Arc<Statistics>) {
    let mut client = net_conf.build_client().await;

    let get_options = task_conf.find_get_options();
    let put_options = task_conf.find_put_options();
    let measure_time = task_conf.is_time_measurement_thread();
    let upper_idx = task_conf.low_idx + task_conf.count;
    let worker_start = Instant::now();
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
        SECOND_PB.inc(1);
        if let Some(time) = &task_conf.time {
            if Instant::now().duration_since(worker_start) > *time {
                break;
            }
        }
    }
}

fn spawn_workers(
    net_conf: &NetConfig,
    task_conf: &TaskConfig,
    benchmark_conf: &BenchmarkConfig,
) -> Vec<tokio::task::JoinHandle<()>> {
    let task_size = task_conf.count / benchmark_conf.workers_count;
    SECOND_PB.set_position(0);
    SECOND_PB.set_length(task_conf.count);
    if let Behavior::Test = benchmark_conf.behavior {
        SECOND_PB.set_length(SECOND_PB.length() * 2);
    }
    (0..benchmark_conf.workers_count)
        .map(|i| {
            let nc = net_conf.clone();
            let stat_inner = benchmark_conf.statistics.clone();
            let low_idx = task_conf.low_idx + task_size * i;
            let count = if i == benchmark_conf.workers_count - 1 {
                task_conf.count + task_conf.low_idx - low_idx
            } else {
                task_size
            };
            let tc = TaskConfig {
                low_idx,
                count,
                payload_size: task_conf.payload_size,
                direct: task_conf.direct,
                measure_time: i == 0,
                time: task_conf.time,
            };
            match benchmark_conf.behavior {
                Behavior::Put => tokio::spawn(put_worker(nc, tc, stat_inner)),
                Behavior::Get => tokio::spawn(get_worker(nc, tc, stat_inner)),
                Behavior::Test => tokio::spawn(test_worker(
                    nc,
                    tc,
                    stat_inner,
                    benchmark_conf.save_name.clone(),
                )),
                Behavior::PingPong => tokio::spawn(ping_pong_worker(nc, tc, stat_inner)),
            }
        })
        .collect()
}

fn save_stat(stats: &Arc<Statistics>, name: Option<&String>) {
    let _lock = SAVE_LOCK.lock().unwrap();
    use std::io::Write;
    let name = match name {
        Some(name) => name,
        _ => return,
    };
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(name);
    match file {
        Ok(mut file) => {
            let time = START.elapsed().as_secs();
            let save_stats = StatisticsToSave::new(stats);
            let save_stats = match serde_json::to_string(&save_stats) {
                Ok(res) => res,
                _ => return,
            };
            if let Err(err) = writeln!(file, "{}", save_stats) {
                LOG_PB.println(&format!("Save log error: {}", err));
                if let Err(err) = file.flush() {
                    LOG_PB.println(&format!("Flush log error: {}", err));
                }
            } else {
                LOG_PB.set_message(&format!("Log saved {}", time));
            }
        }
        Err(err) => {
            LOG_PB.println(&format!("Save log error: {}", err));
        }
    }
}

fn spawn_statistics_thread(
    benchmark_conf: &BenchmarkConfig,
    stop_token: &Arc<AtomicBool>,
) -> JoinHandle<()> {
    let stop_token = stop_token.clone();
    let stat = benchmark_conf.statistics.clone();
    let bytes_amount = benchmark_conf.request_amount_bytes;
    let save_name = benchmark_conf.save_name.clone();
    let save_period_sec = benchmark_conf.save_period_sec;
    let mpb_join = tokio::spawn(async {
        MPB.join_and_clear().unwrap();
    });
    tokio::spawn(stat_worker(
        stop_token,
        1000,
        stat,
        bytes_amount,
        save_name,
        save_period_sec,
        mpb_join,
    ))
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

#[allow(dead_code)]
async fn spawn_disks_worker(stop_token: Arc<AtomicBool>) {
    const REFRESH_RATE: u64 = 5;
    let style = ProgressStyle::default_bar();
    SYSTEM.write().unwrap().refresh_all();
    let sys = SYSTEM.read().unwrap();
    let mut intv = interval(Duration::from_secs(REFRESH_RATE));
    let mut pbs = HashMap::new();
    for disk in sys.get_disks() {
        let pb = MPB.add(ProgressBar::new(100).with_style(style.clone()));
        pb.set_prefix(&format!("{:>15}", disk.get_name().to_str().unwrap()));
        pbs.insert(disk.get_name().to_owned(), pb);
    }
    let mem_pb = MPB.add(ProgressBar::new(sys.get_total_memory()).with_style(style));
    mem_pb.set_prefix(&format!("{:>15}", "MEMORY"));
    mem_pb.set_position(sys.get_used_memory());
    mem_pb.set_message(&format!(
        "{:.2}%",
        sys.get_used_memory() as f64 / sys.get_used_memory() as f64 * 100.0
    ));

    tokio::spawn(async move {
        while !stop_token.load(Ordering::Relaxed) {
            intv.tick().await;
            SYSTEM.write().unwrap().refresh_all();
            let sys = SYSTEM.read().unwrap();
            let disks = sys.get_disks();
            mem_pb.set_position(sys.get_used_memory());
            mem_pb.set_message(&format!(
                "{:.2}%",
                sys.get_used_memory() as f64 / sys.get_total_memory() as f64 * 100.0
            ));
            for disk in disks {
                if let Some(pb) = pbs.get(disk.get_name()) {
                    let total = disk.get_total_space();
                    let avail = disk.get_available_space();
                    let filled = total - avail;
                    pb.set_length(total);
                    pb.set_position(filled);
                    pb.set_message(&format!("{:.2}%", filled as f64 / total as f64 * 100.0));
                }
            }
        }
        for (_, pb) in pbs.into_iter() {
            pb.finish_and_clear();
        }
    });
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
        .arg(
            Arg::with_name("verify")
                .help("verify results of put requests")
                .takes_value(false)
                .long("verify"),
        )
        .arg(
            Arg::with_name("metrics")
                .help("metrics file")
                .takes_value(true)
                .long("metrics")
                .short("m"),
        )
        .arg(
            Arg::with_name("save_period")
                .help("metrics save period(sec)")
                .takes_value(true)
                .long("save-period")
                .default_value("120")
                .short("s"),
        )
        .arg(
            Arg::with_name("repeat")
                .help("Repeat task")
                .takes_value(true)
                .long("repeat")
                .default_value("1")
                .short("r"),
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
