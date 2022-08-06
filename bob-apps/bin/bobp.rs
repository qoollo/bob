use rand::prelude::*;

use bob::{
    Blob, BlobKey, BlobMeta, BobApiClient, ExistRequest, GetOptions, GetRequest, GetSource,
    PutOptions, PutRequest,
};

use clap::{App, Arg, ArgMatches};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::mem::size_of;
use std::ops::Sub;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{self, Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{fs::File, io::{BufReader, Read}};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Channel, Endpoint, Certificate, ClientTlsConfig};
use tonic::{Code, Request, Status};

#[macro_use]
extern crate log;

#[derive(Clone)]
struct NetConfig {
    port: u16,
    target: String,
    ca_cert_path: Option<String>,
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
            ca_cert_path: matches.value_of("ca_path").map(|p| p.to_string()),
        }
    }

    fn load_tls_certificate(path: &str) -> Vec<u8> {
        let f = File::open(path).expect("can not open ca certificate file");
        let mut reader = BufReader::new(f);
        let mut buffer = Vec::new();
        reader
            .read_to_end(&mut buffer)
            .expect("can not read ca certificate from file");
        buffer
    }

    async fn build_client(&self) -> BobApiClient<Channel> {
        let mut endpoint = Endpoint::from(self.get_uri()).tcp_nodelay(true);
        if let Some(ca_cert_path) = &self.ca_cert_path {
            let cert_bin = Self::load_tls_certificate(&ca_cert_path);
            let cert = Certificate::from_pem(cert_bin);
            let tls_config = ClientTlsConfig::new().domain_name("bob").ca_certificate(cert);
            endpoint = endpoint.tls_config(tls_config).expect("tls config");
        }
        loop {
            match BobApiClient::connect(endpoint.clone()).await {
                Ok(client) => return client,
                Err(e) => {
                    sleep(Duration::from_millis(1000)).await;
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

#[derive(Clone, PartialEq)]
enum Mode {
    Normal,
    Random,
}

#[derive(Clone)]
struct TaskConfig {
    low_idx: u64,
    count: u64,
    payload_size: u64,
    direct: bool,
    mode: Mode,
    measure_time: bool,
    key_size: usize,
    basic_username: Option<String>,
    basic_password: Option<String>,
    packet_size: u64,
}

impl FromStr for Mode {
    type Err = &'static str;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "normal" => Ok(Mode::Normal),
            "random" => Ok(Mode::Random),
            _ => Err("Failed to parse mode, only 'random' and 'normal' available"),
        }
    }
}

impl TaskConfig {
    fn from_matches(matches: &ArgMatches) -> Self {
        let mode_string: String = matches.value_or_default("normal");
        let mode = Mode::from_str(&mode_string).unwrap_or(Mode::Normal);
        let key_size = matches.value_or_default("keysize");
        let low_idx = matches.value_or_default("first");
        let count = matches.value_or_default("count");
        if key_size < std::mem::size_of::<u64>() {
            let max_allowed = 256_u64.pow(key_size as u32) - 1;
            if low_idx + count > max_allowed {
                panic!(
                    "max possible index for keysize={} is {}, but task includes keys up to {}",
                    key_size,
                    max_allowed,
                    low_idx + count
                );
            }
        }
        let basic_username = matches.value_of("user").map(|s| s.to_string());
        let basic_password = matches.value_of("password").map(|s| s.to_string());
        let packet_size = matches.value_or_default("packet_size");

        Self {
            low_idx,
            count,
            payload_size: matches.value_or_default("payload"),
            direct: matches.is_present("direct"),
            mode,
            measure_time: false,
            key_size,
            basic_username,
            basic_password,
            packet_size,
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

    fn is_random(&self) -> bool {
        self.mode == Mode::Random
    }

    fn get_proper_key(&self, key: u64) -> Vec<u8> {
        let mut data = key.to_le_bytes().to_vec();
        data.resize(self.key_size, 0);
        data
    }

    fn get_request_creator<T>(&self) -> impl Fn(T) -> Request<T> {
        let do_basic_auth = self.basic_username.is_some() && self.basic_password.is_some();
        let basic_username = self.basic_username.clone().unwrap_or_default();
        let basic_password = self.basic_password.clone().unwrap_or_default();

        let basic_username = basic_username
            .parse::<MetadataValue<Ascii>>()
            .expect("can not parse username into header");
        let basic_password = basic_password
            .parse::<MetadataValue<Ascii>>()
            .expect("can not parse password into header");

        move |a| {
            let mut request = Request::new(a);
            if do_basic_auth {
                let req_md = request.metadata_mut();
                req_md.insert("username", basic_username.clone());
                req_md.insert("password", basic_password.clone());
            }
            request
        }
    }

    fn prepare_exist_keys(&self) -> Vec<Vec<u64>> {
        let mut keys = Vec::new();
        let mut current_low = self.low_idx;
        let shuffle = self.is_random();
        let full_packets = self.count / self.packet_size;
        let tail = self.count - full_packets * self.packet_size;
        for _ in 0..full_packets {
            let mut keys_portion: Vec<_> = 
                (current_low..current_low + self.packet_size).collect();
            if shuffle {
                keys_portion.shuffle(&mut thread_rng());
            }
    
            keys.push(keys_portion);
    
            current_low += self.packet_size;
        }
        if tail > 0 {
            let mut keys_portion: Vec<_> = 
                (current_low..current_low + tail).collect();
            if shuffle {
                keys_portion.shuffle(&mut thread_rng());
            }
    
            keys.push(keys_portion);
        }
        keys
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

    exist_total: AtomicU64,
    exist_error_count: AtomicU64,

    put_time_ns_st: AtomicU64,
    put_count_st: AtomicU64,

    get_time_ns_st: AtomicU64,
    get_count_st: AtomicU64,

    exist_time_ns_st: AtomicU64,
    exist_count_st: AtomicU64,

    get_errors: Mutex<HashMap<CodeRepresentation, u64>>,
    put_errors: Mutex<HashMap<CodeRepresentation, u64>>,
    exist_errors: Mutex<HashMap<CodeRepresentation, u64>>,

    get_size_bytes: AtomicU64,

    exist_size_bytes: AtomicU64,

    exist_presented_keys: AtomicU64,

    verified_puts: AtomicU64,
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

    fn save_single_thread_exist_time(&self, duration: &Duration) {
        self.exist_time_ns_st
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        self.exist_count_st.fetch_add(1, Ordering::Relaxed);
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

    async fn save_exist_error(&self, status: Status) {
        let mut guard = self.exist_errors.lock().await;
        guard
            .entry(status.code() as CodeRepresentation)
            .and_modify(|i| *i += 1)
            .or_insert(1);

        self.exist_error_count.fetch_add(1, Ordering::SeqCst);
        debug!("{}", status.message())
    }
}

struct BenchmarkConfig {
    workers_count: u64,
    behavior: Behavior,
    statistics: Arc<Statistics>,
    time: Option<Duration>,
    request_amount_bytes: u64,
    keys_count: u64,
}

#[derive(Debug)]
enum Behavior {
    Put,
    Get,
    Exist,
    Test,
    PingPong,
}

impl FromStr for Behavior {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "get" => Ok(Behavior::Get),
            "put" => Ok(Behavior::Put),
            "exist" => Ok(Behavior::Exist),
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
            keys_count: matches.value_or_default("count"),
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

    println!("Bob will be benchmarked now");
    println!(
        "target: {:?}\r\nbenchmark configuration: {:?}\r\ntotal task configuration: {:?}",
        net_conf, benchmark_conf, task_conf,
    );

    let stop_token: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));

    let stat_thread = spawn_statistics_thread(&benchmark_conf, &stop_token);

    let workers = spawn_workers(&net_conf, &task_conf, &benchmark_conf);

    if let Some(time) = benchmark_conf.time {
        sleep(time).await;
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
    keys_count: u64,
    behavior_flags: u8,
) {
    let (put_speed_values, get_speed_values, exist_speed_values, elapsed) =
        print_periodic_stat(stop_token, period_ms, &stat, request_bytes, behavior_flags);
    print_averages(&stat, &put_speed_values, &get_speed_values, &exist_speed_values, elapsed, keys_count, behavior_flags);
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
    let guard = stat.exist_errors.lock().await;
    if !guard.is_empty() {
        println!("exist errors:");
        for (&code, count) in guard.iter() {
            println!("{:?} = {}", Code::from(code), count);
        }
    }
}

fn print_averages(
    stat: &Arc<Statistics>,
    put_speed_values: &[f64],
    get_speed_values: &[f64],
    exist_speed_values: &[f64],
    elapsed: Duration,
    keys_count: u64,
    behavior_flags: u8,
) {
    let total_put = stat.put_total.load(Ordering::Relaxed);
    let total_exist = stat.exist_total.load(Ordering::Relaxed);
    let total_get = stat.get_total.load(Ordering::Relaxed);
    let avg_total = ((total_put + total_get + total_exist) * 1000)
                    .checked_div(elapsed.as_millis() as u64)
                    .unwrap_or_default();
    let total_err = stat.put_error_count.load(Ordering::Relaxed) + 
                    stat.get_error_count.load(Ordering::Relaxed) +
                    stat.exist_error_count.load(Ordering::Relaxed);
    let print_put = (behavior_flags & PUT_FLAG) > 0;
    let print_get = (behavior_flags & GET_FLAG) > 0;
    let print_exist = (behavior_flags & EXIST_FLAG) > 0;
    print!("avg total: {} rps | total err: {}\r\n", avg_total, total_err);
    if print_put {
        let put_resp_time = finite_or_default(
            (stat.put_time_ns_st.load(Ordering::Relaxed) as f64)
                / (stat.put_count_st.load(Ordering::Relaxed) as f64)
                / 1e9
        );
        println!("put: {:>6.2} kb/s | resp time {:>6.2} ms", average(put_speed_values), put_resp_time);
    }
    if print_get {
        let get_resp_time = finite_or_default(
            (stat.get_time_ns_st.load(Ordering::Relaxed) as f64)
                / (stat.get_count_st.load(Ordering::Relaxed) as f64)
                / 1e9
        );
        println!("get: {:>6.2} kb/s | resp time {:>6.2} ms", average(get_speed_values), get_resp_time);
    }
    if print_exist {
        let exist_resp_time = finite_or_default(
            (stat.exist_time_ns_st.load(Ordering::Relaxed) as f64)
                / (stat.exist_count_st.load(Ordering::Relaxed) as f64)
                / 1e9
        );
        let present_keys = stat.exist_presented_keys.load(Ordering::Relaxed);
        println!("exist: {:>6.2} kb/s | resp time {:>6.2} ms | {} of {} keys present", 
            average(exist_speed_values), exist_resp_time, present_keys, keys_count);
    }
    if get_matches().is_present("verify") {
        println!(
            "verified put threads: {}",
            stat.verified_puts.load(Ordering::Relaxed)
        );
    }
}

fn print_periodic_stat(
    stop_token: Arc<AtomicBool>,
    period_ms: u64,
    stat: &Arc<Statistics>,
    request_bytes: u64,
    behavior_flags: u8,
) -> (Vec<f64>, Vec<f64>, Vec<f64>, Duration) {
    let pause = time::Duration::from_millis(period_ms);
    let mut put_count = DiffContainer::new(Box::new(|| stat.put_total.load(Ordering::Relaxed)));
    let mut get_count = DiffContainer::new(Box::new(|| stat.get_total.load(Ordering::Relaxed)));
    let mut exist_count = DiffContainer::new(Box::new(|| stat.exist_total.load(Ordering::Relaxed)));
    let mut put_time_st =
        DiffContainer::new(Box::new(|| stat.put_time_ns_st.load(Ordering::Relaxed)));
    let mut put_count_st =
        DiffContainer::new(Box::new(|| stat.put_count_st.load(Ordering::Relaxed)));
    let mut get_time_st =
        DiffContainer::new(Box::new(|| stat.get_time_ns_st.load(Ordering::Relaxed)));
    let mut get_count_st =
        DiffContainer::new(Box::new(|| stat.get_count_st.load(Ordering::Relaxed)));
    let mut exist_time_st =
        DiffContainer::new(Box::new(|| stat.exist_time_ns_st.load(Ordering::Relaxed)));
    let mut exist_count_st =
        DiffContainer::new(Box::new(|| stat.exist_count_st.load(Ordering::Relaxed)));
    let mut get_size = DiffContainer::new(Box::new(|| stat.get_size_bytes.load(Ordering::Relaxed)));
    let mut exist_size = DiffContainer::new(Box::new(|| stat.exist_size_bytes.load(Ordering::Relaxed)));
    let mut put_speed_values = vec![];
    let mut get_speed_values = vec![];
    let mut exist_speed_values = vec![];
    let sec = period_ms as f64 / 1000.;
    let k = request_bytes as f64 / 1024.0;
    let start = Instant::now();
    let print_put = (behavior_flags & PUT_FLAG) > 0;
    let print_get = (behavior_flags & GET_FLAG) > 0;
    let print_exist = (behavior_flags & EXIST_FLAG) > 0;
    while !stop_token.load(Ordering::Relaxed) {
        thread::sleep(pause);
        let elapsed = start.elapsed().as_secs();
        print!("{:>5} ", elapsed);
        let mut first_line = true;
        if print_put {
            if !first_line {
                print!("{:>6}", ' ');
            } else {
                first_line = false;
            }

            let d_put = put_count.get_diff();
            let put_count_spd = d_put * 1000 / period_ms;
            let cur_st_put_time = put_time_st.get_diff() as f64;
            let cur_st_put_count = put_count_st.get_diff() as f64;
            let put_error = stat.put_error_count.load(Ordering::Relaxed);
            let put_spd = put_count_spd as f64 * k;
            println!("put:   {:>6} rps | err {:5} | {:>6.2} kb/s | lat {:>6.2} ms", 
                put_count_spd,
                put_error,
                put_spd,
                finite_or_default(cur_st_put_time / cur_st_put_count / 1e9));

            put_speed_values.push(put_spd);
        }
        if print_get {
            if !first_line {
                print!("{:>6}", ' ');
            } else {
                first_line = false;
            }
            
            let d_get = get_count.get_diff();
            let get_count_spd = d_get * 1000 / period_ms;
            let cur_st_get_time = get_time_st.get_diff() as f64;
            let cur_st_get_count = get_count_st.get_diff() as f64;
            let get_error = stat.get_error_count.load(Ordering::Relaxed);
            let get_spd = get_size.get_diff() as f64 / 1024. / sec;
            println!("get:   {:>6} rps | err {:5} | {:>6.2} kb/s | lat {:>6.2} ms", 
                get_count_spd,
                get_error,
                get_spd,
                finite_or_default(cur_st_get_time / cur_st_get_count / 1e9));

            get_speed_values.push(get_spd);
        }
        if print_exist {
            if !first_line {
                print!("{:>6}", ' ');
            }
            
            let d_exist = exist_count.get_diff();
            let exist_count_spd = d_exist * 1000 / period_ms;
            let cur_st_exist_time = exist_time_st.get_diff() as f64;
            let cur_st_exist_count = exist_count_st.get_diff() as f64;
            let exist_error = stat.exist_error_count.load(Ordering::Relaxed);
            let exist_spd = exist_size.get_diff() as f64 / 1024.0 / sec;
            println!("exist: {:>6} rps | err {:5} | {:>6.2} kb/s | lat {:>6.2} ms", 
                exist_count_spd,
                exist_error,
                exist_spd,
                finite_or_default(cur_st_exist_time / cur_st_exist_count / 1e9));

            exist_speed_values.push(exist_spd);
        }
    }
    let elapsed = start.elapsed();
    println!("Total statistics, elapsed: {:?}", elapsed);
    (put_speed_values, get_speed_values,exist_speed_values, elapsed)
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
    let iterator: Box<dyn Send + Iterator<Item = u64>> = if task_conf.is_random() {
        let mut keys: Vec<_> = (task_conf.low_idx..upper_idx).collect();
        keys.shuffle(&mut thread_rng());
        Box::new(keys.into_iter())
    } else {
        Box::new(task_conf.low_idx..upper_idx)
    };

    let request_creator = task_conf.get_request_creator::<GetRequest>();
    for key in iterator {
        let request = request_creator(GetRequest {
            key: Some(BlobKey {
                key: task_conf.get_proper_key(key),
            }),
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
    }
}

async fn put_worker(net_conf: NetConfig, task_conf: TaskConfig, stat: Arc<Statistics>) {
    let mut client = net_conf.build_client().await;

    let request_creator = task_conf.get_request_creator::<PutRequest>();

    let options: Option<PutOptions> = task_conf.find_put_options();
    let measure_time = task_conf.is_time_measurement_thread();
    let upper_idx = task_conf.low_idx + task_conf.count;
    for i in task_conf.low_idx..upper_idx {
        let blob = create_blob(&task_conf);
        let key = BlobKey {
            key: task_conf.get_proper_key(i),
        };
        let request = request_creator(PutRequest {
            key: Some(key),
            data: Some(blob),
            options: options.clone(),
        });

        let res = if measure_time {
            let start = Instant::now();
            let res = client.put(request).await;
            stat.save_single_thread_put_time(&start.elapsed());
            res
        } else {
            client.put(request).await
        };
        if let Err(status) = res {
            stat.save_put_error(status).await;
        }
        stat.put_total.fetch_add(1, Ordering::SeqCst);
    }
    let req = Request::new(ExistRequest {
        keys: (task_conf.low_idx..upper_idx)
            .map(|i| BlobKey {
                key: task_conf.get_proper_key(i),
            })
            .collect(),
        options: task_conf.find_get_options(),
    });
    if get_matches().is_present("verify") {
        let res = client.exist(req).await;
        if let Ok(res) = res {
            if res.into_inner().exist.iter().all(|b| *b) {
                stat.verified_puts.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

async fn exist_worker(net_conf: NetConfig, task_conf: TaskConfig, stat: Arc<Statistics>) {
    let mut client = net_conf.build_client().await;

    let request_creator = task_conf.get_request_creator::<ExistRequest>();

    let options = task_conf.find_get_options();
    let send_size_bytes = task_conf.packet_size * (task_conf.key_size as u64);
    let measure_time = task_conf.is_time_measurement_thread();

    let keys = task_conf.prepare_exist_keys();
    let iterator = Box::new(keys.into_iter());

    for portion in iterator {
        let keys = portion.iter().map(|key| 
            BlobKey {
                key: task_conf.get_proper_key(*key),
            }).collect();
        let request = request_creator(ExistRequest {
            keys,
            options: options.clone(),
        });
        let res = if measure_time {
            let start = Instant::now();
            let res = client.exist(request).await;
            stat.save_single_thread_exist_time(&start.elapsed());
            res
        } else {
            client.exist(request).await
        };
        stat.exist_size_bytes
                    .fetch_add(send_size_bytes, Ordering::SeqCst);
        match res {
            Err(status) => stat.save_exist_error(status).await,
            Ok(payload) => {
                let res = payload.into_inner().exist;
                stat.exist_size_bytes
                    .fetch_add((res.len() * size_of::<bool>()) as u64, Ordering::SeqCst);

                let present = res.iter().fold(0, |acc, flag| acc + u64::from(*flag));
                stat.exist_presented_keys
                    .fetch_add(present, Ordering::SeqCst);
            }
        }
        stat.exist_total.fetch_add(1, Ordering::SeqCst);
    }
}

async fn test_worker(net_conf: NetConfig, task_conf: TaskConfig, stat: Arc<Statistics>) {
    put_worker(net_conf.clone(), task_conf.clone(), stat.clone()).await;
    get_worker(net_conf.clone(), task_conf.clone(), stat.clone()).await;
    exist_worker(net_conf, task_conf, stat).await;
}

async fn ping_pong_worker(net_conf: NetConfig, task_conf: TaskConfig, stat: Arc<Statistics>) {
    let mut client = net_conf.build_client().await;

    let get_request_creator = task_conf.get_request_creator::<GetRequest>();
    let put_request_creator = task_conf.get_request_creator::<PutRequest>();

    let get_options = task_conf.find_get_options();
    let put_options = task_conf.find_put_options();
    let measure_time = task_conf.is_time_measurement_thread();
    let upper_idx = task_conf.low_idx + task_conf.count;
    for i in task_conf.low_idx..upper_idx {
        let blob = create_blob(&task_conf);
        let key = BlobKey {
            key: task_conf.get_proper_key(i),
        };
        let put_request = put_request_creator(PutRequest {
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
        let get_request = get_request_creator(GetRequest {
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
                mode: task_conf.mode.clone(),
                measure_time: i == 0,
                key_size: task_conf.key_size,
                basic_username: task_conf.basic_username.clone(),
                basic_password: task_conf.basic_password.clone(),
                packet_size: task_conf.packet_size,
            };
            match benchmark_conf.behavior {
                Behavior::Put => tokio::spawn(put_worker(nc, tc, stat_inner)),
                Behavior::Get => tokio::spawn(get_worker(nc, tc, stat_inner)),
                Behavior::Exist => tokio::spawn(exist_worker(nc, tc, stat_inner)),
                Behavior::Test => tokio::spawn(test_worker(nc, tc, stat_inner)),
                Behavior::PingPong => tokio::spawn(ping_pong_worker(nc, tc, stat_inner)),
            }
        })
        .collect()
}

const EXIST_FLAG: u8 = 4;
const PUT_FLAG: u8 = 1;
const GET_FLAG: u8 = 2;
fn spawn_statistics_thread(
    benchmark_conf: &BenchmarkConfig,
    stop_token: &Arc<AtomicBool>,
) -> JoinHandle<()> {
    let stop_token = stop_token.clone();
    let stat = benchmark_conf.statistics.clone();
    let bytes_amount = benchmark_conf.request_amount_bytes;
    let keys_count = benchmark_conf.keys_count;
    let mut behavior_flags = 0;
    match benchmark_conf.behavior {
        Behavior::Exist => {
            behavior_flags |= EXIST_FLAG;
        },
        Behavior::Get => {
            behavior_flags |= GET_FLAG;
        },
        Behavior::PingPong => {
            behavior_flags |= GET_FLAG;
            behavior_flags |= PUT_FLAG;
        },
        Behavior::Put => {
            behavior_flags |= PUT_FLAG;
        },
        Behavior::Test => {
            behavior_flags |= GET_FLAG;
            behavior_flags |= PUT_FLAG;
            behavior_flags |= EXIST_FLAG;
        }
    }
    tokio::spawn(stat_worker(stop_token, 1000, stat, bytes_amount, keys_count, behavior_flags))
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
                .help("put / get / exist / test")
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
            Arg::with_name("mode")
                .help("random (keys in get operation are shuffled) or normal")
                .takes_value(true)
                .default_value("normal")
                .long("mode"),
        )
        .arg(
            Arg::with_name("keysize")
                .help("size of the binary key")
                .takes_value(true)
                .long("keysize")
                .short("k")
                .default_value(option_env!("BOB_KEY_SIZE").unwrap_or("8")),
        )
        .arg(
            Arg::with_name("user")
                .help("username for auth")
                .takes_value(true)
                .long("user"),
        )
        .arg(
            Arg::with_name("password")
                .help("password for auth")
                .takes_value(true)
                .long("password"),
        )
        .arg(
            Arg::with_name("packet_size")
                .help("size of packet in exist request")
                .takes_value(true)
                .long("packet_size")
                .short("s")
                .default_value("1000"),
        )
        .arg(
            Arg::with_name("ca_path")
                .help("path to tls ca certificate")
                .takes_value(true)
                .long("ca_path"),
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
