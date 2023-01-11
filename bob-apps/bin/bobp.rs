use rand::prelude::*;

use bob::{
    Blob, BlobKey, BlobMeta, BobApiClient, ExistRequest, GetOptions, GetRequest, GetSource,
    PutOptions, PutRequest, DeleteRequest, DeleteOptions,
};

use clap::{App, Arg, ArgMatches};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::fs;
use std::mem::size_of;
use std::ops::Sub;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{self, Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tonic::{Code, Request, Status};

#[macro_use]
extern crate log;

#[derive(Clone)]
struct NetConfig {
    port: u16,
    target: String,
    ca_cert_path: Option<String>,
    tls_domain_name: Option<String>,
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
            tls_domain_name: matches.value_of("domain_name").map(|n| n.to_string()),
        }
    }

    async fn build_client(&self) -> BobApiClient<Channel> {
        let mut endpoint = Endpoint::from(self.get_uri()).tcp_nodelay(true);
        if let Some(ca_cert_path) = &self.ca_cert_path {
            let cert_bin = fs::read(&ca_cert_path).expect("can not read ca certificate from file");
            let cert = Certificate::from_pem(cert_bin);
            let domain_name = self.tls_domain_name.as_ref().expect("domain name required");
            let tls_config = ClientTlsConfig::new()
                .domain_name(domain_name)
                .ca_certificate(cert);
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

    fn find_delete_options(&self) -> Option<DeleteOptions> {
        // option is mandatory for delete request to work
        if self.direct {
            Some(DeleteOptions {
                force_node: true,
                is_alien: false,
                force_alien_nodes: vec![],
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

        let credentials = format!("{}:{}", basic_username, basic_password);
        let credentials = base64::encode(credentials);
        let authorization = format!("Basic {}", credentials)
            .parse::<MetadataValue<Ascii>>()
            .expect("can not parse authorization value");

        move |a| {
            let mut request = Request::new(a);
            if do_basic_auth {
                let req_md = request.metadata_mut();
                req_md.insert("authorization", authorization.clone());
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
            let mut keys_portion: Vec<_> = (current_low..current_low + self.packet_size).collect();
            if shuffle {
                keys_portion.shuffle(&mut thread_rng());
            }

            keys.push(keys_portion);

            current_low += self.packet_size;
        }
        if tail > 0 {
            let mut keys_portion: Vec<_> = (current_low..current_low + tail).collect();
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

struct PeriodicStatisticsBit {
    count_spd: u64,
    error: u64,
    latency: f64,
}

struct PeriodicOperationStatistics<'a> {
    count: DiffContainer<'a, u64>,
    time_st: DiffContainer<'a, u64>,
    count_st: DiffContainer<'a, u64>,
    period_ms: u64,
    stat: &'a OperationStatistics
}

impl<'a> PeriodicOperationStatistics<'a> {
    fn from_operation_statistics(os: &'a OperationStatistics, period_ms: u64) -> Self {
        Self {
            count: DiffContainer::new(Box::new(|| os.total.load(Ordering::Relaxed))),
            time_st: DiffContainer::new(Box::new(|| os.time_ns_st.load(Ordering::Relaxed))),
            count_st: DiffContainer::new(Box::new(|| os.count_st.load(Ordering::Relaxed))),
            period_ms,
            stat: os,
        }
    }

    fn current_bit(&mut self) -> PeriodicStatisticsBit {
        let diff = self.count.get_diff();
        let count_spd = diff * 1000 / self.period_ms;
        let cur_st_time = self.time_st.get_diff() as f64;
        let cur_st_count = self.count_st.get_diff() as f64;
        let error = self.stat.error_count.load(Ordering::Relaxed);
        let latency = finite_or_default(cur_st_time / cur_st_count / 1e9);
        PeriodicStatisticsBit {
            count_spd,
            error,
            latency,
        }
    }
}

struct PeriodicPutStatistics<'a> {
    common: PeriodicOperationStatistics<'a>,
    request_size_kb: f64,
}

impl<'a> PeriodicPutStatistics<'a> {
    fn from(ps: &'a PutStatistics, period_ms: u64, request_size_b: u64) -> Self {
        Self {
            common: PeriodicOperationStatistics::from_operation_statistics(&ps.common, period_ms),
            request_size_kb: request_size_b as f64 / 1024.,
        }
    }

    fn process_and_print(&mut self) -> f64 {
        let bit = self.common.current_bit();
        let spd = bit.count_spd as f64 * self.request_size_kb;
        println!("put:   {:>6} rps | err {:5} | {:>6.2} kb/s | lat {:>6.2} ms", 
            bit.count_spd, bit.error, spd, bit.latency);
        spd
    }
}

struct PeriodicDeleteStatistics<'a> {
    common: PeriodicOperationStatistics<'a>,
    request_size_kb: f64,
}

impl<'a> PeriodicDeleteStatistics<'a> {
    fn from(ds: &'a DeleteStatistics, period_ms: u64, request_size_b: u64) -> Self {
        Self {
            common: PeriodicOperationStatistics::from_operation_statistics(&ds.common, period_ms),
            request_size_kb: request_size_b as f64 / 1024.,
        }
    }

    fn process_and_print(&mut self) -> f64 {
        let bit = self.common.current_bit();
        let spd = bit.count_spd as f64 * self.request_size_kb;
        println!("delete:{:>6} rps | err {:5} | {:>6.2} kb/s | lat {:>6.2} ms", 
            bit.count_spd, bit.error, spd, bit.latency);
        spd
    }
}

struct PeriodicGetStatistics<'a> {
    common: PeriodicOperationStatistics<'a>,
    size: DiffContainer<'a, u64>,
    period_s: f64,
}

impl<'a> PeriodicGetStatistics<'a> {
    fn from(gs: &'a GetStatistics, period_ms: u64) -> Self {
        Self {
            common: PeriodicOperationStatistics::from_operation_statistics(&gs.common, period_ms),
            size: DiffContainer::new(Box::new(|| gs.size_bytes.load(Ordering::Relaxed))),
            period_s: period_ms as f64 / 1000.,
        }
    }

    fn process_and_print(&mut self) -> f64 {
        let bit = self.common.current_bit();
        let spd = self.size.get_diff() as f64 / 1024. / self.period_s;
        println!("get:   {:>6} rps | err {:5} | {:>6.2} kb/s | lat {:>6.2} ms", 
            bit.count_spd, bit.error, spd, bit.latency);
        spd
    }
}

struct PeriodicExistStatistics<'a> {
    common: PeriodicOperationStatistics<'a>,
    size: DiffContainer<'a, u64>,
    period_s: f64,
}

impl<'a> PeriodicExistStatistics<'a> {
    fn from(es: &'a ExistStatistics, period_ms: u64) -> Self {
        Self {
            common: PeriodicOperationStatistics::from_operation_statistics(&es.common, period_ms),
            size: DiffContainer::new(Box::new(|| es.size_bytes.load(Ordering::Relaxed))),
            period_s: period_ms as f64 / 1000.,
        }
    }

    fn process_and_print(&mut self) -> f64 {
        let bit = self.common.current_bit();
        let spd = self.size.get_diff() as f64 / 1024.0 / self.period_s;
        println!("exist: {:>6} rps | err {:5} | {:>6.2} kb/s | lat {:>6.2} ms", 
            bit.count_spd, bit.error, spd, bit.latency);
        spd
    }
}

type CodeRepresentation = i32; // Because tonic::Code does not implement hash

#[derive(Default)]
struct OperationStatistics {
    total: AtomicU64,
    error_count: AtomicU64,
    time_ns_st: AtomicU64,
    count_st: AtomicU64,
    errors: Mutex<HashMap<CodeRepresentation, u64>>,
}

impl OperationStatistics {
    async fn save_error(&self, status: Status) {
        let mut guard = self.errors.lock().expect("mutex");
        guard
            .entry(status.code() as CodeRepresentation)
            .and_modify(|i| *i += 1)
            .or_insert(1);

        self.error_count.fetch_add(1, Ordering::SeqCst);
        debug!("{}", status.message())
    }

    fn save_single_thread_time(&self, duration: &Duration) {
        self.time_ns_st
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        self.count_st.fetch_add(1, Ordering::Relaxed);
    }

    fn print_errors(&self, operation_name: &str) {
        let guard = self.errors.lock().expect("mutex");
        if !guard.is_empty() {
            println!("{} errors:", operation_name);
            for (&code, count) in guard.iter() {
                println!("{:?} = {}", Code::from(code), count);
            }
        }
    }

    fn get_average_response_time(&self) -> f64 {
        finite_or_default(
            (self.time_ns_st.load(Ordering::Relaxed) as f64)
                / (self.count_st.load(Ordering::Relaxed) as f64)
                / 1e9
        )
    }
}

#[derive(Default)]
struct PutStatistics {
    common: OperationStatistics,
    verified: AtomicU64,
}

#[derive(Default)]
struct GetStatistics {
    common: OperationStatistics,
    size_bytes: AtomicU64,
}

#[derive(Default)]
struct ExistStatistics {
    common: OperationStatistics,
    size_bytes: AtomicU64,
    presented_keys: AtomicU64,
}

#[derive(Default)]
struct DeleteStatistics {
    common: OperationStatistics,
    verified: AtomicU64,
}

#[derive(Default)]
struct Statistics {
    put: Arc<PutStatistics>,
    get: Arc<GetStatistics>,
    exist: Arc<ExistStatistics>,
    delete: Arc<DeleteStatistics>,
}

impl Statistics {
    fn get_total_op_count(&self) -> u64 {
        let total_put = self.put.common.total.load(Ordering::Relaxed);
        let total_exist = self.exist.common.total.load(Ordering::Relaxed);
        let total_get = self.get.common.total.load(Ordering::Relaxed);
        let total_delete = self.delete.common.total.load(Ordering::Relaxed);
        total_put + total_get + total_exist + total_delete
    }

    fn get_total_error_count(&self) -> u64 {
        self.put.common.error_count.load(Ordering::Relaxed) + 
        self.get.common.error_count.load(Ordering::Relaxed) +
        self.delete.common.error_count.load(Ordering::Relaxed) +
        self.exist.common.error_count.load(Ordering::Relaxed)
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
    Delete,
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
            "delete" => Ok(Behavior::Delete),
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
    let (put_speed_values, get_speed_values, exist_speed_values, delete_speed_values, elapsed) =
        print_periodic_stat(stop_token, period_ms, &stat, request_bytes, behavior_flags);
    print_averages(&stat, &put_speed_values, &get_speed_values, &exist_speed_values, &delete_speed_values, elapsed, keys_count, behavior_flags);
    print_errors_with_codes(stat).await
}

async fn print_errors_with_codes(stat: Arc<Statistics>) {
    stat.get.common.print_errors("get");
    stat.put.common.print_errors("put");
    stat.exist.common.print_errors("exist");
    stat.delete.common.print_errors("delete");
}

fn print_averages(
    stat: &Arc<Statistics>,
    put_speed_values: &[f64],
    get_speed_values: &[f64],
    exist_speed_values: &[f64],
    delete_speed_values: &[f64],
    elapsed: Duration,
    keys_count: u64,
    behavior_flags: u8,
) {
    let avg_total = (stat.get_total_op_count() * 1000)
        .checked_div(elapsed.as_millis() as u64)
        .unwrap_or_default();
    let total_err = stat.get_total_error_count();
    print!("avg total: {} rps | total err: {}\r\n", avg_total, total_err);
    let verify = get_matches().is_present("verify");
    if (behavior_flags & PUT_FLAG) > 0 {
        let put_resp_time = stat.put.common.get_average_response_time();
        println!("put: {:>6.2} kb/s | resp time {:>6.2} ms", average(put_speed_values), put_resp_time);

        if verify {
            println!(
                "verified put threads: {}",
                stat.put.verified.load(Ordering::Relaxed)
            );
        }
    }
    if (behavior_flags & GET_FLAG) > 0 {
        let get_resp_time = stat.get.common.get_average_response_time();
        println!("get: {:>6.2} kb/s | resp time {:>6.2} ms", average(get_speed_values), get_resp_time);
    }
    if (behavior_flags & DELETE_FLAG) > 0 {
        let delete_resp_time = stat.delete.common.get_average_response_time();
        println!("delete: {:>6.2} kb/s | resp time {:>6.2} ms", average(delete_speed_values), delete_resp_time);

        if verify {
            println!(
                "verified delete threads: {}",
                stat.delete.verified.load(Ordering::Relaxed)
            );
        }
    }
    if (behavior_flags & EXIST_FLAG) > 0 {
        let exist_resp_time = stat.exist.common.get_average_response_time();
        let present_keys = stat.exist.presented_keys.load(Ordering::Relaxed);
        println!("exist: {:>6.2} kb/s | resp time {:>6.2} ms | {} of {} keys present", 
            average(exist_speed_values), exist_resp_time, present_keys, keys_count);
    }
}

fn print_offset(first_line: bool) -> bool {
    if !first_line {
        print!("{:>6}", ' ');
    }
    false
}

fn print_periodic_stat(
    stop_token: Arc<AtomicBool>,
    period_ms: u64,
    stat: &Arc<Statistics>,
    request_bytes: u64,
    behavior_flags: u8,
) -> (Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>, Duration) {
    let pause = time::Duration::from_millis(period_ms);
    let mut put_speed_values = vec![];
    let mut get_speed_values = vec![];
    let mut exist_speed_values = vec![];
    let mut delete_speed_values = vec![];

    let mut periodic_put = PeriodicPutStatistics::from(&stat.put, period_ms, request_bytes);
    let mut periodic_get = PeriodicGetStatistics::from(&stat.get, period_ms);
    let mut periodic_exist = PeriodicExistStatistics::from(&stat.exist, period_ms);
    let mut periodic_delete = PeriodicDeleteStatistics::from(&stat.delete, period_ms, request_bytes);

    let start = Instant::now();
    let print_put = (behavior_flags & PUT_FLAG) > 0;
    let print_get = (behavior_flags & GET_FLAG) > 0;
    let print_exist = (behavior_flags & EXIST_FLAG) > 0;
    let print_delete = (behavior_flags & DELETE_FLAG) > 0;
    while !stop_token.load(Ordering::Relaxed) {
        thread::sleep(pause);
        let elapsed = start.elapsed().as_secs();
        print!("{:>5} ", elapsed);
        let mut first_line = true;
        if print_put {
            first_line = print_offset(first_line);
            let put_spd = periodic_put.process_and_print();
            put_speed_values.push(put_spd);
        }
        if print_get {
            first_line = print_offset(first_line);
            let get_spd = periodic_get.process_and_print();
            get_speed_values.push(get_spd);
        }
        if print_delete {
            first_line = print_offset(first_line);
            let delete_spd = periodic_delete.process_and_print();
            delete_speed_values.push(delete_spd);
        }
        if print_exist {
            print_offset(first_line);
            let exist_spd = periodic_exist.process_and_print();
            exist_speed_values.push(exist_spd);
        }
    }
    let elapsed = start.elapsed();
    println!("Total statistics, elapsed: {:?}", elapsed);
    (
        put_speed_values,
        get_speed_values,
        exist_speed_values,
        delete_speed_values,
        elapsed,
    )
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

async fn get_worker(net_conf: NetConfig, task_conf: TaskConfig, stat: Arc<GetStatistics>) {
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
            stat.common.save_single_thread_time(&start.elapsed());
            res
        } else {
            client.get(request).await
        };
        match res {
            Err(status) => stat.common.save_error(status).await,
            Ok(payload) => {
                let res = payload.into_inner().data;
                stat.size_bytes
                    .fetch_add(res.len() as u64, Ordering::SeqCst);
            }
        }
        stat.common.total.fetch_add(1, Ordering::SeqCst);
    }
}

async fn put_worker(net_conf: NetConfig, task_conf: TaskConfig, stat: Arc<PutStatistics>) {
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
            stat.common.save_single_thread_time(&start.elapsed());
            res
        } else {
            client.put(request).await
        };
        if let Err(status) = res {
            stat.common.save_error(status).await;
        }
        stat.common.total.fetch_add(1, Ordering::SeqCst);
    }
    if get_matches().is_present("verify") {
        let request_creator = task_conf.get_request_creator::<ExistRequest>();
        let req = request_creator(
            ExistRequest {
                keys: (task_conf.low_idx..upper_idx)
                    .map(|i| BlobKey {
                        key: task_conf.get_proper_key(i),
                    })
                    .collect(),
                options: task_conf.find_get_options(),
            });
        let res = client.exist(req).await;
        if let Ok(res) = res {
            if res.into_inner().exist.iter().all(|b| *b) {
                stat.verified.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

async fn delete_worker(net_conf: NetConfig, task_conf: TaskConfig, stat: Arc<DeleteStatistics>) {
    let mut client = net_conf.build_client().await;

    let request_creator = task_conf.get_request_creator::<DeleteRequest>();

    let options = task_conf.find_delete_options();
    let measure_time = task_conf.is_time_measurement_thread();
    let upper_idx = task_conf.low_idx + task_conf.count;
    for i in task_conf.low_idx..upper_idx {
        let key = BlobKey {
            key: task_conf.get_proper_key(i),
        };
        let request = request_creator(DeleteRequest {
            key: Some(key),
            options: options.clone(),
            meta: None,
        });

        let res = if measure_time {
            let start = Instant::now();
            let res = client.delete(request).await;
            stat.common.save_single_thread_time(&start.elapsed());
            res
        } else {
            client.delete(request).await
        };
        if let Err(status) = res {
            stat.common.save_error(status).await;
        }
        stat.common.total.fetch_add(1, Ordering::SeqCst);
    }
    if get_matches().is_present("verify") {
        let request_creator = task_conf.get_request_creator::<ExistRequest>();
        let req = request_creator(
            ExistRequest {
                keys: (task_conf.low_idx..upper_idx)
                    .map(|i| BlobKey {
                        key: task_conf.get_proper_key(i),
                    })
                    .collect(),
                options: task_conf.find_get_options(),
            });
        let res = client.exist(req).await;
        if let Ok(res) = res {
            if res.into_inner().exist.iter().all(|b| !(*b)) {
                stat.verified.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

async fn exist_worker(net_conf: NetConfig, task_conf: TaskConfig, stat: Arc<ExistStatistics>) {
    let mut client = net_conf.build_client().await;

    let request_creator = task_conf.get_request_creator::<ExistRequest>();

    let options = task_conf.find_get_options();
    let send_size_bytes = task_conf.packet_size * (task_conf.key_size as u64);
    let measure_time = task_conf.is_time_measurement_thread();

    let keys = task_conf.prepare_exist_keys();
    let iterator = Box::new(keys.into_iter());

    for portion in iterator {
        let keys = portion
            .iter()
            .map(|key| BlobKey {
                key: task_conf.get_proper_key(*key),
            })
            .collect();
        let request = request_creator(ExistRequest {
            keys,
            options: options.clone(),
        });
        let res = if measure_time {
            let start = Instant::now();
            let res = client.exist(request).await;
            stat.common.save_single_thread_time(&start.elapsed());
            res
        } else {
            client.exist(request).await
        };
        stat.size_bytes
                    .fetch_add(send_size_bytes, Ordering::SeqCst);
        match res {
            Err(status) => stat.common.save_error(status).await,
            Ok(payload) => {
                let res = payload.into_inner().exist;
                stat.size_bytes
                    .fetch_add((res.len() * size_of::<bool>()) as u64, Ordering::SeqCst);

                let present = res.iter().fold(0, |acc, flag| acc + u64::from(*flag));
                stat.presented_keys
                    .fetch_add(present, Ordering::SeqCst);
            }
        }
        stat.common.total.fetch_add(1, Ordering::SeqCst);
    }
}

async fn test_worker(net_conf: NetConfig, task_conf: TaskConfig, stat: Arc<Statistics>) {
    put_worker(net_conf.clone(), task_conf.clone(), stat.put.clone()).await;
    get_worker(net_conf.clone(), task_conf.clone(), stat.get.clone()).await;
    exist_worker(net_conf.clone(), task_conf.clone(), stat.exist.clone()).await;
    delete_worker(net_conf, task_conf, stat.delete.clone()).await;
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
            stat.put.common.save_single_thread_time(&start.elapsed());
            res
        } else {
            client.put(put_request).await
        };
        if let Err(status) = put_res {
            stat.put.common.save_error(status).await;
        }
        stat.put.common.total.fetch_add(1, Ordering::SeqCst);
        let get_request = get_request_creator(GetRequest {
            key: Some(key),
            options: get_options.clone(),
        });
        let get_res = if measure_time {
            let start = Instant::now();
            let res = client.get(get_request).await;
            stat.get.common.save_single_thread_time(&start.elapsed());
            res
        } else {
            client.get(get_request).await
        };
        if let Err(status) = get_res {
            stat.get.common.save_error(status).await;
        }
        stat.get.common.total.fetch_add(1, Ordering::SeqCst);
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
                Behavior::Put => tokio::spawn(put_worker(nc, tc, stat_inner.put.clone())),
                Behavior::Get => tokio::spawn(get_worker(nc, tc, stat_inner.get.clone())),
                Behavior::Exist => tokio::spawn(exist_worker(nc, tc, stat_inner.exist.clone())),
                Behavior::Delete => tokio::spawn(delete_worker(nc, tc, stat_inner.delete.clone())),
                Behavior::Test => tokio::spawn(test_worker(nc, tc, stat_inner)),
                Behavior::PingPong => tokio::spawn(ping_pong_worker(nc, tc, stat_inner)),
            }
        })
        .collect()
}

const DELETE_FLAG: u8 = 8;
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
        }
        Behavior::Get => {
            behavior_flags |= GET_FLAG;
        },
        Behavior::Delete => {
            behavior_flags |= DELETE_FLAG;
        },
        Behavior::PingPong => {
            behavior_flags |= GET_FLAG;
            behavior_flags |= PUT_FLAG;
        }
        Behavior::Put => {
            behavior_flags |= PUT_FLAG;
        }
        Behavior::Test => {
            behavior_flags |= GET_FLAG;
            behavior_flags |= PUT_FLAG;
            behavior_flags |= EXIST_FLAG;
            behavior_flags |= DELETE_FLAG;
        }
    }
    tokio::spawn(stat_worker(
        stop_token,
        1000,
        stat,
        bytes_amount,
        keys_count,
        behavior_flags,
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
        data: vec![0_u8; task_conf.payload_size as usize].into(),
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
                .help("put / get / exist / delete / test")
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
                .long("ca_path")
                .requires("domain_name"),
        )
        .arg(
            Arg::with_name("domain_name")
                .help("tls domain name")
                .takes_value(true)
                .long("domain_name"),
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
