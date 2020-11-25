use std::io::{self};
use metrics::{Key, Recorder, SetRecorderError};
use std::time::{ Duration };
use std::sync::mpsc::{ channel, Sender };
use std::thread;
use log::{ error };

mod retry_socket;
mod send;
use send::send_metrics;

const DEFAULT_ADDRESS: &str = "localhost:2003";
const DEFAULT_DURATION: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub(crate) enum Error {
    Io(io::Error),
    Recorder(SetRecorderError),
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
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
        MetricInner { key, value, timestamp }
    }
}

pub(crate) struct GraphiteRecorder {
    tx: Sender<Metric>,
}

pub(crate) struct GraphiteBuilder {
    address: String,
    interval: Duration,
}

impl GraphiteBuilder {
    pub(crate) fn new() -> GraphiteBuilder {
        GraphiteBuilder {
            address: DEFAULT_ADDRESS.to_string(),
            interval: DEFAULT_DURATION,
        }
    }

    pub(crate) fn set_interval(mut self, interval: Duration) -> GraphiteBuilder {
        self.interval = interval;
        self
    }

    pub(crate) fn set_address(mut self, addr: String) -> GraphiteBuilder
    {
        self.address = addr;
        self
    }

    pub(crate) fn install(self) -> Result<(), Error> {
        let recorder = self.build()?;
        metrics::set_boxed_recorder(Box::new(recorder))?;
        Ok(())
    }

    pub(crate) fn build(self) -> Result<GraphiteRecorder, Error> {
        let (tx, rx) = channel();
        let recorder = GraphiteRecorder {
            tx,
        };
        thread::spawn(move || send_metrics(rx, self.address, self.interval));
        Ok(recorder)
    }
}


impl GraphiteRecorder {
    fn push_metric(&self, m: Metric) {
        if let Err(e) = self.tx.send(m) {
            error!("Can't send metric to thread, which processing metrics: {}", e);
        }
    }
}

impl Recorder for GraphiteRecorder {
    fn increment_counter(&self, key: Key, value: u64) {
        self.push_metric(Metric::Counter(MetricInner::new(key.name().into_owned(), value, -1)));
    }

    fn update_gauge(&self, key: Key, value: i64) {
        self.push_metric(Metric::Gauge(MetricInner::new(key.name().into_owned(), value as u64, -1)));
    }

    fn record_histogram(&self, key: Key, value: u64) {
        self.push_metric(Metric::Time(MetricInner::new(key.name().into_owned(), value, -1)));
    }
}
