use metrics::{GaugeValue, Key, Recorder};
use tokio::sync::mpsc::{Sender, error::TrySendError};
use std::{fmt::{Display, Formatter, Result as FMTResult}};

use super::snapshot::{Metric, MetricInner};
use crate::interval_logger::IntervalLoggerSafe;

const ERROR_LOG_INTERVAL_MS: u64 = 5000;

#[derive(Hash, PartialEq, Eq)]
enum PushMetricError {
    Closed,
    NoCapacity,
}

impl Display for PushMetricError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FMTResult {
        match self {
            PushMetricError::Closed =>
                write!(f, "Can't send metric to thread, channel is closed"),
            PushMetricError::NoCapacity =>
                write!(f, "Can't send metric to thread, channel is full"),
        }
    }
}

pub(crate) struct MetricsRecorder {
    tx: Sender<Metric>,
    error_logger: IntervalLoggerSafe<PushMetricError>
}

impl MetricsRecorder {
    pub(super) fn new(tx: Sender<Metric>) -> Self {
        Self {
            tx,
            error_logger: IntervalLoggerSafe::new(ERROR_LOG_INTERVAL_MS, log::Level::Error),
        }
    }

    fn push_metric(&self, m: Metric) {
        if let Err(e) = self.tx.try_send(m) {
            match e {
                TrySendError::Closed(_) => self.error_logger.report_error(PushMetricError::Closed),
                TrySendError::Full(_) => self.error_logger.report_error(PushMetricError::NoCapacity),
            }
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
            -1,
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
            -1,
        )));
    }

    fn record_histogram(&self, key: &Key, value: f64) {
        self.push_metric(Metric::Time(MetricInner::new(
            key.name().to_owned(),
            value as u64,
            -1,
        )));
    }
}
