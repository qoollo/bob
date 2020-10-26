/// Component responsible for working with I/O.
pub mod backend;
/// GRPC client to deal with backend.
pub mod bob_client;
pub(crate) mod cluster;
/// Configuration tools.
pub mod configs;
pub mod data;
pub(crate) mod error;
/// Component to manage cluster I/O and connections.
pub mod grinder;
pub(crate) mod link_manager;
/// Component to map storage space on disks.
pub mod mapper;
/// Tools for tracking bob different indicators.
pub mod metrics;
pub mod node;
/// GRPC server to receive and process requests from clients.
pub mod server;
/// Component to cleanup memory
pub(crate) mod cleaner;

pub(crate) use self::error::Error;
pub(crate) use super::prelude::*;
pub(crate) use backend::{init_pearl, Backend, Operation};

mod prelude {
    pub(crate) use super::*;

    pub(crate) use bob_client::{BobClient, Factory};
    pub(crate) use cluster::{get_cluster, Cluster};
    pub(crate) use configs::{Cluster as ClusterConfig, Node as NodeConfig};
    pub(crate) use data::{BobData, BobFlags, BobKey, BobMeta, BobOptions, DiskPath, VDiskID};
    pub(crate) use dipstick::{
        AtomicBucket, Counter, Graphite, Input, InputKind, InputScope, MetricName, MetricValue,
        Prefixed, Proxy, ScheduleFlush, ScoreType, TimeHandle, Timer,
    };
    pub(crate) use futures::{
        future, stream::FuturesUnordered, Future, FutureExt, StreamExt, TryFutureExt,
    };
    pub(crate) use grinder::Grinder;
    pub(crate) use grpc::{
        bob_api_server::BobApi, ExistRequest, ExistResponse, GetOptions, GetSource, Null, OpStatus,
        PutOptions,
    };
    pub(crate) use http::Uri;
    pub(crate) use link_manager::LinkManager;
    pub(crate) use mapper::Virtual;
    pub(crate) use metrics::{
        BobClient as BobClientMetrics, ContainerBuilder as MetricsContainerBuilder,
        CLIENT_GET_COUNTER, CLIENT_GET_ERROR_COUNT_COUNTER, CLIENT_GET_TIMER, CLIENT_PUT_COUNTER,
        CLIENT_PUT_ERROR_COUNT_COUNTER, CLIENT_PUT_TIMER, GRINDER_GET_COUNTER,
        GRINDER_GET_ERROR_COUNT_COUNTER, GRINDER_GET_TIMER, GRINDER_PUT_COUNTER,
        GRINDER_PUT_ERROR_COUNT_COUNTER, GRINDER_PUT_TIMER,
    };
    pub(crate) use node::{Disk as NodeDisk, Node, Output as NodeOutput, ID as NodeID};
    pub(crate) use stopwatch::Stopwatch;
    pub(crate) use termion::color;
    pub(crate) use tokio::{
        net::lookup_host,
        time::{interval, delay_for, timeout},
    };
    pub(crate) use tonic::{
        transport::{Channel, Endpoint},
        Code, Request, Response, Status,
    };
    pub(crate) use cleaner::Cleaner;
}

#[cfg(test)]
pub(crate) mod test_utils {
    use super::{
        bob_client::{GetResult, PingResult, PutResult},
        prelude::*,
    };
    use chrono::Local;
    use env_logger::fmt::{Color, Formatter as EnvFormatter};
    use log::{Level, Record};
    use std::io::Write;

    pub(crate) fn ping_ok(node_name: String) -> PingResult {
        Ok(NodeOutput::new(node_name, ()))
    }

    pub(crate) fn put_ok(node_name: String) -> PutResult {
        Ok(NodeOutput::new(node_name, ()))
    }

    pub(crate) fn put_err(node_name: String) -> PutResult {
        debug!("return internal error on PUT");
        Err(NodeOutput::new(node_name, Error::internal()))
    }

    pub(crate) fn get_ok(node_name: String, timestamp: u64) -> GetResult {
        let inner = BobData::new(vec![], BobMeta::new(timestamp));
        Ok(NodeOutput::new(node_name, inner))
    }

    pub(crate) fn get_err(node_name: String) -> GetResult {
        debug!("return internal error on GET");
        Err(NodeOutput::new(node_name, Error::internal()))
    }

    #[allow(dead_code)]
    pub(crate) fn init_logger() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Debug)
            .format(logger_format)
            .try_init();
    }

    fn logger_format(buf: &mut EnvFormatter, record: &Record) -> IOResult<()> {
        {
            let mut style = buf.style();
            let color = match record.level() {
                Level::Error => Color::Red,
                Level::Warn => Color::Yellow,
                Level::Info => Color::Green,
                Level::Debug => Color::Cyan,
                Level::Trace => Color::White,
            };
            style.set_color(color);
            writeln!(
                buf,
                "[{} {:>24}:{:^4} {:^5}] - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.module_path().unwrap_or(""),
                record.line().unwrap_or(0),
                style.value(record.level()),
                record.args(),
            )
        }
    }
}
