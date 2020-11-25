use std::io::{Write};
use std::collections::HashMap;
use std::time::{ Duration, Instant };
use std::sync::mpsc::{ Receiver };
use log::{ debug, trace };

use super::retry_socket::RetrySocket;
use super::{ MetricKey, MetricInner, Metric, MetricValue, TimeStamp };

// this function runs in other thread, so it would be better if it will take control of arguments
// themselves, not just references
#[allow(clippy::needless_pass_by_value)]
pub(super) fn send_metrics(rx: Receiver<Metric>, address: String, send_interval: Duration) {
    let mut socket = RetrySocket::new(&address).expect("Failed to resolve address from &str");
    let mut counters_map = HashMap::new();
    let mut gauges_map = HashMap::new();
    let mut times_map = HashMap::new();
    let mut stopwatch = Instant::now();

    loop {
        for m in rx.try_iter() {
            match m {
                Metric::Counter(counter) => process_counter(&mut counters_map, counter),
                Metric::Gauge(gauge) => process_gauge(&mut gauges_map, gauge),
                Metric::Time(time) => process_time(&mut times_map, time),
            }
        }

        if stopwatch.elapsed() > send_interval {
            flush_counters(&counters_map, &mut socket);
            flush_gauges(&gauges_map, &mut socket);
            flush_times(&mut times_map, &mut socket);
            if let Err(e) = socket.flush() {
                debug!("Socket flush error: {}", e);
            }
            stopwatch = Instant::now();
        }
    }
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
        Self {
            value, timestamp,
        }
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
    let entry = counters_map.entry(counter.key).or_insert(CounterEntry::new(counter.timestamp));
    entry.sum += counter.value;
    entry.timestamp = counter.timestamp;
}

fn process_gauge(gauges_map: &mut HashMap<MetricKey, GaugeEntry>, gauge: MetricInner) {
    gauges_map.insert(gauge.key, GaugeEntry::new(gauge.value, gauge.timestamp));
}

fn process_time(times_map: &mut HashMap<MetricKey, TimeEntry>, time: MetricInner) {
    let entry = times_map.entry(time.key).or_insert(TimeEntry::new(time.timestamp));
    entry.summary_time += time.value;
    entry.measurements_amount += 1;
    entry.timestamp = time.timestamp;
}

fn flush_counters(counters_map: &HashMap<MetricKey, CounterEntry>, socket: &mut RetrySocket) {
    for (key, entry) in counters_map.iter() {
        let data = format!("{} {} {}\n", key, entry.sum, entry.timestamp);
        trace!("Counter data: {:<30} {:<20} {:<20}", key, entry.sum, entry.timestamp);
        if let Err(e) = socket.write(data.as_bytes()) {
            debug!("Can't write counter data to socket: {}", e);
        }
    }
}

fn flush_gauges(gauges_map: &HashMap<MetricKey, GaugeEntry>, socket: &mut RetrySocket) {
    for (key, entry) in gauges_map.iter() {
        let data = format!("{} {} {}\n", key, entry.value, entry.timestamp);
        trace!("Gauge   data: {:<30} {:<20} {:<20}", key, entry.value, entry.timestamp);
        if let Err(e) = socket.write(data.as_bytes()) {
            debug!("Can't write gauge data to socket: {}", e);
        }
    }
}

fn flush_times(times_map: &mut HashMap<MetricKey, TimeEntry>, socket: &mut RetrySocket) {
    for (key, entry) in times_map.iter_mut() {
        let mean_time = match entry.measurements_amount {
            0 => entry.mean.expect("No mean time provided"),
            val => entry.summary_time / val,
        };
        let data = format!("{} {} {}\n", key, mean_time, entry.timestamp);
        trace!("Time    data: {:<30} {:<20} {:<20}", key, mean_time, entry.timestamp);
        if let Err(e) = socket.write(data.as_bytes()) {
            debug!("Can't write time data to socket: {}", e);
        }
        entry.mean = Some(mean_time);
        entry.measurements_amount = 0;
        entry.summary_time = 0;
    }
}
