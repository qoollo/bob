use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::sync::RwLock;
use tokio::time::{interval, timeout};

use super::{Metric, MetricInner, MetricKey, MetricValue, TimeStamp};

const METRICS_RECV_TIMEOUT: Duration = Duration::from_millis(100);

pub(crate) type SharedMetricsSnapshot = Arc<RwLock<MetricsSnapshot>>;

#[derive(Default, Clone)]
pub(crate) struct MetricsSnapshot {
    pub(crate) counters_map: HashMap<MetricKey, CounterEntry>,
    pub(crate) gauges_map: HashMap<MetricKey, GaugeEntry>,
    pub(crate) times_map: HashMap<MetricKey, TimeEntry>,
}

impl MetricsSnapshot {
    fn process_counter(&mut self, counter: MetricInner) {
        let entry = self
            .counters_map
            .entry(counter.key.clone())
            .or_insert_with(|| CounterEntry::new(counter.timestamp));
        entry.sum += counter.value;
        entry.timestamp = counter.timestamp;
    }
    fn process_gauge(&mut self, gauge: MetricInner) {
        self.gauges_map
            .insert(gauge.key, GaugeEntry::new(gauge.value, gauge.timestamp));
    }

    fn process_time(&mut self, time: MetricInner) {
        let entry = self
            .times_map
            .entry(time.key.clone())
            .or_insert_with(|| TimeEntry::new(time.timestamp));
        entry.summary_time += time.value;
        entry.measurements_amount += 1;
        entry.timestamp = time.timestamp;
    }

    fn update_and_get_moment_snapshot(&mut self) -> Self {
        for (_, entry) in self.times_map.iter_mut() {
            let mean_time = match entry.measurements_amount {
                0 => entry.mean.expect("No mean time provided"),
                val => entry.summary_time / val,
            };
            entry.mean = Some(mean_time);
            entry.measurements_amount = 0;
            entry.summary_time = 0;
        }
        let res = self.clone();
        res
    }
}

pub(crate) struct MetricsAccumulator {
    rx: Receiver<Metric>,
    interval: Duration,
    snapshot: MetricsSnapshot,
    readable_snapshot: SharedMetricsSnapshot,
}

impl MetricsAccumulator {
    pub(super) fn new(rx: Receiver<Metric>, interval: Duration) -> Self {
        Self {
            rx,
            interval,
            snapshot: MetricsSnapshot::default(),
            readable_snapshot: Arc::default(),
        }
    }
    // this function runs in other thread, so it would be better if it will take control of arguments
    // themselves, not just references
    #[allow(clippy::needless_pass_by_value)]
    pub(crate) async fn run(mut self) {
        let mut interval = interval(self.interval);

        loop {
            interval.tick().await;
            while let Ok(m) = timeout(METRICS_RECV_TIMEOUT, self.rx.recv()).await {
                match m {
                    Some(Metric::Counter(counter)) => self.snapshot.process_counter(counter),
                    Some(Metric::Gauge(gauge)) => self.snapshot.process_gauge(gauge),
                    Some(Metric::Time(time)) => self.snapshot.process_time(time),
                    // if recv returns None, then sender is dropped, then no more metrics would come
                    None => return,
                }
            }

            let mut w = self.readable_snapshot.write().await;
            *w = self.snapshot.update_and_get_moment_snapshot();
        }
    }

    pub(crate) fn get_shared_snapshot(&self) -> SharedMetricsSnapshot {
        self.readable_snapshot.clone()
    }
}

#[derive(Clone)]
pub struct CounterEntry {
    pub sum: MetricValue,
    pub timestamp: TimeStamp,
}

impl CounterEntry {
    fn new(timestamp: TimeStamp) -> Self {
        Self { sum: 0, timestamp }
    }
}

#[derive(Clone)]
pub struct GaugeEntry {
    pub value: MetricValue,
    pub timestamp: TimeStamp,
}

impl GaugeEntry {
    fn new(value: MetricValue, timestamp: TimeStamp) -> Self {
        Self { value, timestamp }
    }
}

#[derive(Clone)]
pub struct TimeEntry {
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
