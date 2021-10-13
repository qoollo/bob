use std::{
    convert::TryInto,
    time::{SystemTime, UNIX_EPOCH},
};

use metrics::{GaugeValue, Key, Recorder};
use tokio::sync::mpsc::Sender;

use super::snapshot::{Metric, MetricInner};

pub(crate) struct MetricsRecorder {
    tx: Sender<Metric>,
}

impl MetricsRecorder {
    pub(super) fn new(tx: Sender<Metric>) -> Self {
        Self { tx }
    }

    fn push_metric(&self, m: Metric) {
        if let Err(e) = self.tx.try_send(m) {
            error!(
                "Can't send metric to thread, which processing metrics: {}",
                e
            );
        }
    }
}

// TODO: impl recorder in proper way
impl Recorder for MetricsRecorder {
    fn register_gauge(
        &self,
        key: &Key,
        _unit: Option<metrics::Unit>,
        _description: Option<&'static str>,
    ) {
        self.update_gauge(key, GaugeValue::Absolute(0f64));
    }

    fn register_counter(
        &self,
        key: &Key,
        _unit: Option<metrics::Unit>,
        _description: Option<&'static str>,
    ) {
        self.increment_counter(key, 0);
    }

    fn register_histogram(
        &self,
        key: &Key,
        _unit: Option<metrics::Unit>,
        _description: Option<&'static str>,
    ) {
        self.record_histogram(key, 0f64);
    }

    fn increment_counter(&self, key: &Key, value: u64) {
        self.push_metric(Metric::Counter(MetricInner::new(
            key.name().to_owned(),
            value,
            get_current_unix_timestamp(),
        )));
    }

    #[allow(clippy::cast_sign_loss)]
    fn update_gauge(&self, key: &Key, value: GaugeValue) {
        let val = if let GaugeValue::Absolute(val) = value {
            val
        } else {
            error!("Diffs are not supported at the moment");
            return;
        };
        self.push_metric(Metric::Gauge(MetricInner::new(
            key.name().to_owned(),
            val as u64,
            get_current_unix_timestamp(),
        )));
    }

    fn record_histogram(&self, key: &Key, value: f64) {
        self.push_metric(Metric::Time(MetricInner::new(
            key.name().to_owned(),
            value as u64,
            get_current_unix_timestamp(),
        )));
    }
}

fn get_current_unix_timestamp() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(n) => n.as_secs().try_into().expect("timestamp conversion"),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}
