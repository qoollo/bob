use crate::metrics::snapshot::{CounterEntry, GaugeEntry, MetricKey, TimeEntry};
use crate::metrics::SharedMetricsSnapshot;
use log::{debug, trace};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::interval;

use super::retry_socket::RetrySocket;

// this function runs in other thread, so it would be better if it will take control of arguments
// themselves, not just references
#[allow(clippy::needless_pass_by_value)]
pub(super) async fn send_metrics(
    metrics: SharedMetricsSnapshot,
    address: String,
    send_interval: Duration,
    prefix: String,
) {
    let mut socket =
        RetrySocket::new(address.parse().expect("Can't read address from String")).await;
    let mut send_interval = interval(send_interval);

    loop {
        send_interval.tick().await;

        if socket.check_connection().is_ok() {
            let r = metrics.read().await;
            flush_counters(&r.counters_map, &mut socket, &prefix).await;
            flush_gauges(&r.gauges_map, &mut socket, &prefix).await;
            flush_times(&r.times_map, &mut socket, &prefix).await;
            if let Err(e) = socket.flush().await {
                debug!("Socket flush error: {}", e);
            }
        }
    }
}

async fn flush_counters(
    counters_map: &HashMap<MetricKey, CounterEntry>,
    socket: &mut RetrySocket,
    prefix: &str,
) {
    for (key, entry) in counters_map.iter() {
        let data = format!("{}.{} {} {}\n", prefix, key, entry.sum, entry.timestamp);
        trace!(
            "Counter data: {:<30} {:<20} {:<20}",
            key,
            entry.sum,
            entry.timestamp
        );
        if let Err(e) = socket.write_all(data.as_bytes()).await {
            debug!("Can't write counter data to socket: {}", e);
        }
    }
}

async fn flush_gauges(
    gauges_map: &HashMap<MetricKey, GaugeEntry>,
    socket: &mut RetrySocket,
    prefix: &str,
) {
    for (key, entry) in gauges_map.iter() {
        let data = format!("{}.{} {} {}\n", prefix, key, entry.value, entry.timestamp);
        trace!(
            "Gauge   data: {:<30} {:<20} {:<20}",
            key,
            entry.value,
            entry.timestamp
        );
        if let Err(e) = socket.write_all(data.as_bytes()).await {
            debug!("Can't write gauge data to socket: {}", e);
        }
    }
}

async fn flush_times(
    times_map: &HashMap<MetricKey, TimeEntry>,
    socket: &mut RetrySocket,
    prefix: &str,
) {
    for (key, entry) in times_map.iter() {
        let mean_time = match entry.measurements_amount {
            0 => entry.mean.expect("No mean time provided"),
            val => entry.summary_time / val,
        };
        let data = format!("{}.{} {} {}\n", prefix, key, mean_time, entry.timestamp);
        trace!(
            "Time    data: {:<30} {:<20} {:<20}",
            key,
            mean_time,
            entry.timestamp
        );
        if let Err(e) = socket.write_all(data.as_bytes()).await {
            debug!("Can't write time data to socket: {}", e);
        }
    }
}
