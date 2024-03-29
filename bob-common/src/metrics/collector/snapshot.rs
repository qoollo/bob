use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub type SharedMetricsSnapshot = Arc<RwLock<MetricsSnapshot>>;

#[derive(Default, Clone, Debug, Serialize)]
pub struct MetricsSnapshot {
    pub counters_map: HashMap<MetricKey, CounterEntry>,
    pub gauges_map: HashMap<MetricKey, GaugeEntry>,
    pub times_map: HashMap<MetricKey, TimeEntry>,
}

impl MetricsSnapshot {
    pub(super) fn process_counter(&mut self, counter: MetricInner) {
        let entry = self
            .counters_map
            .entry(counter.key.clone())
            .or_insert_with(|| CounterEntry::new(counter.timestamp));
        entry.sum += counter.value;
        entry.timestamp = counter.timestamp;
    }
    pub(super) fn process_gauge(&mut self, gauge: MetricInner) {
        self.gauges_map
            .insert(gauge.key, GaugeEntry::new(gauge.value, gauge.timestamp));
    }

    pub(super) fn process_time(&mut self, time: MetricInner) {
        let entry = self
            .times_map
            .entry(time.key.clone())
            .or_insert_with(|| TimeEntry::new(time.timestamp));
        entry.summary_time += time.value;
        entry.measurements_amount += 1;
        entry.timestamp = time.timestamp;
    }

    pub(super) fn update_and_get_moment_snapshot(&mut self) -> Self {
        for (_, entry) in self.times_map.iter_mut() {
            entry.mean = match entry.measurements_amount {
                0 => None,
                val => Some(entry.summary_time / val),
            };
            entry.measurements_amount = 0;
            entry.summary_time = 0;
        }
        self.to_owned()
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct CounterEntry {
    pub sum: MetricValue,
    pub timestamp: TimeStamp,
}

impl CounterEntry {
    pub(super) fn new(timestamp: TimeStamp) -> Self {
        Self { sum: 0, timestamp }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct GaugeEntry {
    pub value: MetricValue,
    pub timestamp: TimeStamp,
}

impl GaugeEntry {
    pub(super) fn new(value: MetricValue, timestamp: TimeStamp) -> Self {
        Self { value, timestamp }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct TimeEntry {
    pub summary_time: MetricValue,
    pub measurements_amount: u64,
    pub timestamp: TimeStamp,
    pub mean: Option<MetricValue>,
}

impl TimeEntry {
    pub(super) fn new(timestamp: TimeStamp) -> Self {
        Self {
            summary_time: 0,
            measurements_amount: 1,
            timestamp,
            mean: None,
        }
    }
}

pub type MetricKey = String;
type TimeStamp = i64;
type MetricValue = u64;

pub(super) enum Metric {
    Gauge(MetricInner),
    Counter(MetricInner),
    Time(MetricInner),
}

impl Metric {
    pub(super) fn with_timestamp(self, timestamp: TimeStamp) -> Self {
        self.map(|m| m.with_timestamp(timestamp))
    }

    fn map(self, f: impl Fn(MetricInner) -> MetricInner) -> Self {
        match self {
            Metric::Gauge(m) => Metric::Gauge(f(m)),
            Metric::Counter(m) => Metric::Counter(f(m)),
            Metric::Time(m) => Metric::Time(f(m)),
        }
    }
}

pub(super) struct MetricInner {
    key: MetricKey,
    value: MetricValue,
    timestamp: TimeStamp,
}

impl MetricInner {
    pub(super) fn new(key: MetricKey, value: MetricValue, timestamp: TimeStamp) -> MetricInner {
        MetricInner {
            key,
            value,
            timestamp,
        }
    }

    fn with_timestamp(self, timestamp: TimeStamp) -> Self {
        Self {
            key: self.key,
            value: self.value,
            timestamp,
        }
    }
}
