use metrics::SetRecorderError;
use send::send_metrics;
use std::{io::Error as IOError, time::Duration};

use crate::metrics::SharedMetricsSnapshot;

mod retry_socket;
mod send;

const DEFAULT_ADDRESS: &str = "127.0.0.1:2003";
const DEFAULT_PREFIX: &str = "node.127_0_0_1";
const DEFAULT_DURATION: Duration = Duration::from_secs(1);

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

    // TODO move recorder and snapshot to other place?
    pub(crate) fn build(self, metrics: SharedMetricsSnapshot) {
        tokio::spawn(send_metrics(
            metrics,
            self.address,
            self.interval,
            self.prefix,
        ));
    }
}
