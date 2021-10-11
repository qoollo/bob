use std::collections::HashMap;

use bob_common::metrics::collector::snapshot::{
    CounterEntry, GaugeEntry, MetricsSnapshot, TimeEntry,
};

type TimeStamp = i64; // We need i64 to return -1 in some cases

#[derive(Serialize)]
pub struct MetricsSnapshotModel {
    pub counters: HashMap<String, CounterEntryModel>,
    pub gauges: HashMap<String, GaugeEntryModel>,
    pub times: HashMap<String, TimeEntryModel>,
}

#[derive(Serialize)]
pub struct CounterEntryModel {
    pub sum: u64,
    pub timestamp: TimeStamp,
}

#[derive(Serialize)]
pub struct GaugeEntryModel {
    pub value: u64,
    pub timestamp: TimeStamp,
}

#[derive(Serialize)]
pub struct TimeEntryModel {
    pub summary_time: u64,
    pub measurements_amount: u64,
    pub timestamp: TimeStamp,
    pub mean: Option<u64>,
}

impl From<MetricsSnapshot> for MetricsSnapshotModel {
    fn from(s: MetricsSnapshot) -> Self {
        Self {
            counters: convert_values(s.counters_map),
            gauges: convert_values(s.gauges_map),
            times: convert_values(s.times_map),
        }
    }
}

fn convert_values<V>(src: HashMap<String, impl Into<V>>) -> HashMap<String, V> {
    src.into_iter().map(|(k, v)| (k, v.into())).collect()
}

impl From<CounterEntry> for CounterEntryModel {
    fn from(e: CounterEntry) -> Self {
        Self {
            sum: e.sum,
            timestamp: e.timestamp,
        }
    }
}

impl From<GaugeEntry> for GaugeEntryModel {
    fn from(e: GaugeEntry) -> Self {
        Self {
            value: e.value,
            timestamp: e.timestamp,
        }
    }
}

impl From<TimeEntry> for TimeEntryModel {
    fn from(e: TimeEntry) -> Self {
        Self {
            summary_time: e.summary_time,
            measurements_amount: e.measurements_amount,
            timestamp: e.timestamp,
            mean: e.mean,
        }
    }
}
