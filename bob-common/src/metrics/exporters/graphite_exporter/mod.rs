use metrics::{GaugeValue, Key, Recorder, SetRecorderError};
use std::{io::Error as IOError, time::Duration};
use tokio::sync::mpsc::{channel, Sender};

mod retry_socket;
mod send;
use send::send_metrics;

const DEFAULT_ADDRESS: &str = "127.0.0.1:2003";
const DEFAULT_PREFIX: &str = "node.127_0_0_1";
const DEFAULT_DURATION: Duration = Duration::from_secs(1);
const BUFFER_SIZE: usize = 1_048_576; // 1 Mb

#[derive(Debug)]
pub(crate) enum Error {
    Io(IOError),
    Recorder(SetRecorderError),
}

impl From<IOError> for Error {
    fn from(e: IOError) -> Self {
        Error::Io(e)
    }
}

impl From<SetRecorderError> for Error {
    fn from(e: SetRecorderError) -> Self {
        Error::Recorder(e)
    }
}

enum Metric {
    Gauge(MetricInner),
    Counter(MetricInner),
    Time(MetricInner),
}

type MetricKey = String;
type TimeStamp = i64;
type MetricValue = u64;

struct MetricInner {
    key: MetricKey,
    value: MetricValue,
    timestamp: TimeStamp,
}

impl MetricInner {
    fn new(key: MetricKey, value: MetricValue, timestamp: TimeStamp) -> MetricInner {
        MetricInner {
            key,
            value,
            timestamp,
        }
    }
}

pub(crate) struct GraphiteRecorder {
    tx: Sender<Metric>,
}

pub(crate) struct GraphiteBuilder {
    address: String,
    interval: Duration,
    prefix: String,
}

impl GraphiteBuilder {
    pub(crate) fn new() -> GraphiteBuilder {
        GraphiteBuilder {
            address: DEFAULT_ADDRESS.to_owned(),
            interval: DEFAULT_DURATION,
            prefix: DEFAULT_PREFIX.to_owned(),
        }
    }

    pub(crate) fn set_interval(mut self, interval: Duration) -> GraphiteBuilder {
        self.interval = interval;
        self
    }

    pub(crate) fn set_prefix(mut self, prefix: String) -> GraphiteBuilder {
        self.prefix = prefix;
        self
    }

    pub(crate) fn set_address(mut self, addr: String) -> GraphiteBuilder {
        self.address = addr;
        self
    }

    pub(crate) fn build(self) -> GraphiteRecorder {
        let (tx, rx) = channel(BUFFER_SIZE);
        let recorder = GraphiteRecorder { tx };
        tokio::spawn(send_metrics(rx, self.address, self.interval, self.prefix));
        recorder
    }
}

impl GraphiteRecorder {
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
impl Recorder for GraphiteRecorder {
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
            todo!("Diffs are not supported at the moment");
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
