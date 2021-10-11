use std::collections::HashMap;

use bob_common::metrics::collector::snapshot::{
    CounterEntry, GaugeEntry, MetricsSnapshot, TimeEntry,
};

type TimeStamp = i64; // We need i64 to return -1 in some cases

#[derive(Serialize)]
pub struct MetricsSnapshotModel {
    pub metrics: HashMap<String, MetricsEntryModel>,
}

#[derive(Serialize)]
pub struct MetricsEntryModel {
    pub value: u64,
    pub timestamp: TimeStamp,
}

impl From<MetricsSnapshot> for MetricsSnapshotModel {
    fn from(s: MetricsSnapshot) -> Self {
        let metrics = s
            .counters_map
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .chain(s.gauges_map.into_iter().map(|(k, v)| (k, v.into())))
            .chain(s.times_map.into_iter().map(|(k, v)| (k, v.into())))
            .collect();
        Self { metrics }
    }
}

impl From<CounterEntry> for MetricsEntryModel {
    fn from(e: CounterEntry) -> Self {
        Self {
            value: e.sum,
            timestamp: e.timestamp,
        }
    }
}

impl From<GaugeEntry> for MetricsEntryModel {
    fn from(e: GaugeEntry) -> Self {
        Self {
            value: e.value,
            timestamp: e.timestamp,
        }
    }
}

impl From<TimeEntry> for MetricsEntryModel {
    fn from(e: TimeEntry) -> Self {
        Self {
            value: e.mean.unwrap_or_default(),
            timestamp: e.timestamp,
        }
    }
}
