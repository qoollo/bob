use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::timeout;

use super::retry_socket::RetrySocket;
use super::{Metric, MetricInner, MetricKey, MetricValue, TimeStamp};

const TCP_SENDER_BUFFER_SIZE: usize = 1024;

// this function runs in other thread, so it would be better if it will take control of arguments
// themselves, not just references
#[allow(clippy::needless_pass_by_value)]
pub(super) async fn send_metrics(
    mut rx: Receiver<Metric>,
    address: String,
    send_interval: Duration,
    prefix: String,
) {
    let socket_sender = spawn_tcp_sender_task(address).await;
    let mut counters_map = HashMap::new();
    let mut gauges_map = HashMap::new();
    let mut times_map = HashMap::new();

    loop {
        let next_flush = Instant::now() + send_interval;
        let mut current_time = Instant::now();
        while current_time < next_flush {
            if let Ok(m) = timeout(next_flush - current_time, rx.recv()).await {
                match m {
                    Some(Metric::Counter(counter)) => process_counter(&mut counters_map, counter),
                    Some(Metric::Gauge(gauge)) => process_gauge(&mut gauges_map, gauge),
                    Some(Metric::Time(time)) => process_time(&mut times_map, time),
                    // if recv returns None, then sender is dropped, then no more metrics would come
                    None => return,
                }
            }
            current_time = Instant::now();
        }
        let mut res_string = String::new();
        let ts = chrono::Local::now().timestamp();
        flush_counters(&counters_map, &mut res_string, &prefix, ts).await;
        flush_gauges(&gauges_map, &mut res_string, &prefix, ts).await;
        flush_times(&mut times_map, &mut res_string, &prefix, ts).await;
        if let Err(e) = socket_sender.send(res_string).await {
            warn!("Can't send data to tcp sender task (reason: {})", e);
        };
    }
}

async fn spawn_tcp_sender_task(address: String) -> Sender<String> {
    let (tx, rx) = channel(TCP_SENDER_BUFFER_SIZE);
    let socket = RetrySocket::new(address.parse().expect("Can't read address from String")).await;
    tokio::spawn(tcp_sender_task(socket, rx));
    tx
}

async fn tcp_sender_task(mut socket: RetrySocket, mut rx: Receiver<String>) {
    while let Some(data) = rx.recv().await {
        if let Err(e) = socket.write_all(data.as_bytes()).await {
            warn!(
                "Can't write data to socket (reason: {}, data: {:?})",
                e, data
            );
            continue;
        }
        if let Err(e) = socket.flush().await {
            warn!("Can't flush socket (reason: {})", e);
        }
    }
    info!("Metrics thread is done.");
}

struct CounterEntry {
    pub sum: MetricValue,
    pub timestamp: TimeStamp,
}

impl CounterEntry {
    fn new(timestamp: TimeStamp) -> Self {
        Self { sum: 0, timestamp }
    }
}

struct GaugeEntry {
    pub value: MetricValue,
    pub timestamp: TimeStamp,
}

impl GaugeEntry {
    fn new(value: MetricValue, timestamp: TimeStamp) -> Self {
        Self { value, timestamp }
    }
}

struct TimeEntry {
    pub summary_time: MetricValue,
    pub measurements_amount: u64,
    pub timestamp: TimeStamp,
    pub mean: Option<MetricValue>,
}

impl TimeEntry {
    fn new(timestamp: TimeStamp) -> Self {
        Self {
            summary_time: 0,
            measurements_amount: 1,
            timestamp,
            mean: None,
        }
    }
}

fn process_counter(counters_map: &mut HashMap<MetricKey, CounterEntry>, counter: MetricInner) {
    let MetricInner {
        key,
        value,
        timestamp,
    } = counter;
    let entry = counters_map
        .entry(key)
        .or_insert_with(|| CounterEntry::new(timestamp));
    entry.sum += value;
    entry.timestamp = timestamp;
}

fn process_gauge(gauges_map: &mut HashMap<MetricKey, GaugeEntry>, gauge: MetricInner) {
    gauges_map.insert(gauge.key, GaugeEntry::new(gauge.value, gauge.timestamp));
}

fn process_time(times_map: &mut HashMap<MetricKey, TimeEntry>, time: MetricInner) {
    let MetricInner {
        key,
        value,
        timestamp,
    } = time;
    let entry = times_map
        .entry(key)
        .or_insert_with(|| TimeEntry::new(timestamp));
    entry.summary_time += value;
    entry.measurements_amount += 1;
    entry.timestamp = timestamp;
}

async fn flush_counters(
    counters_map: &HashMap<MetricKey, CounterEntry>,
    res_string: &mut String,
    prefix: &str,
    ts: i64,
) {
    for (key, entry) in counters_map.iter() {
        let data = format!("{}.{} {} {}\n", prefix, key, entry.sum, ts);
        trace!(
            "Counter data: {:<30} {:<20} {:<20}",
            key,
            entry.sum,
            entry.timestamp
        );
        res_string.push_str(&data);
    }
}

async fn flush_gauges(
    gauges_map: &HashMap<MetricKey, GaugeEntry>,
    res_string: &mut String,
    prefix: &str,
    ts: i64,
) {
    for (key, entry) in gauges_map.iter() {
        let data = format!("{}.{} {} {}\n", prefix, key, entry.value, ts);
        trace!(
            "Gauge   data: {:<30} {:<20} {:<20}",
            key,
            entry.value,
            entry.timestamp
        );
        res_string.push_str(&data);
    }
}

async fn flush_times(
    times_map: &mut HashMap<MetricKey, TimeEntry>,
    res_string: &mut String,
    prefix: &str,
    ts: i64,
) {
    for (key, entry) in times_map.iter_mut() {
        let mean_time = match entry.measurements_amount {
            0 => entry.mean.expect("No mean time provided"),
            val => entry.summary_time / val,
        };
        let data = format!("{}.{} {} {}\n", prefix, key, mean_time, ts);
        trace!(
            "Time    data: {:<30} {:<20} {:<20}",
            key,
            mean_time,
            entry.timestamp
        );
        res_string.push_str(&data);
        entry.mean = Some(mean_time);
        entry.measurements_amount = 0;
        entry.summary_time = 0;
    }
}
