use crate::metrics::collector::snapshot::{CounterEntry, GaugeEntry, MetricKey, TimeEntry};
use crate::metrics::SharedMetricsSnapshot;
use log::trace;
use std::collections::HashMap;

use super::retry_socket::RetrySocket;
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::interval;

const TCP_SENDER_BUFFER_SIZE: usize = 1024;

// this function runs in other thread, so it would be better if it will take control of arguments
// themselves, not just references
#[allow(clippy::needless_pass_by_value)]
pub(super) async fn send_metrics(
    metrics: SharedMetricsSnapshot,
    address: String,
    check_interval: Duration,
    prefix: String,
) {
    let socket_sender = spawn_tcp_sender_task(address).await;
    let mut interval = interval(check_interval);
    loop {
        interval.tick().await;
        let mut res_string = String::new();
        let ts = chrono::Local::now().timestamp();
        {
            let l = metrics.read().expect("rwlock");
            flush_counters(&l.counters_map, &mut res_string, &prefix, ts);
            flush_gauges(&l.gauges_map, &mut res_string, &prefix, ts);
            flush_times(&l.times_map, &mut res_string, &prefix, ts);
        }
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
            warn!("Can't write data to Graphite socket: {}", e);
            continue;
        }
        if let Err(e) = socket.flush().await {
            warn!("Can't flush Graphite socket (reason: {})", e);
        }
    }
    info!("Metrics thread is done.");
}

fn flush_counters(
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

fn flush_gauges(
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

fn flush_times(
    times_map: &HashMap<MetricKey, TimeEntry>,
    res_string: &mut String,
    prefix: &str,
    ts: i64,
) {
    for (key, entry) in times_map.iter() {
        let mean_time = entry.mean.unwrap_or_default();
        let data = format!("{}.{} {} {}\n", prefix, key, mean_time, ts);
        trace!(
            "Time    data: {:<30} {:<20} {:<20}",
            key,
            mean_time,
            entry.timestamp
        );
        res_string.push_str(&data);
    }
}
