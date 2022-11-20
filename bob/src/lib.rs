#![type_length_limit = "3245934"]
#![allow(clippy::used_underscore_binding)]
#![warn(missing_debug_implementations)]
// #![warn(clippy::pedantic)]
// #![warn(missing_docs)]

//! Library requires tokio runtime.

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate metrics;

pub mod api;
pub mod build_info;
pub mod cleaner;
pub mod cluster;
pub mod counter;
pub mod grinder;
pub mod hw_metrics_collector;
pub mod link_manager;
pub mod server;

pub use crate::{grinder::Grinder, server::Server as BobServer};
pub use bob_backend::pearl::Key as PearlKey;
pub use bob_common::{
    bob_client::{Factory, FactoryTlsConfig},
    configs::cluster::{
        Cluster as ClusterConfig, Node as ClusterNodeConfig, Rack as ClusterRackConfig,
        Replica as ReplicaConfig, VDisk as VDiskConfig,
    },
    configs::node::{BackendType, Node as NodeConfig},
    data::BOB_KEY_SIZE,
    mapper::Virtual as VirtualMapper,
    metrics::init_counters,
};
pub use bob_grpc::{
    bob_api_client::BobApiClient, bob_api_server::BobApiServer, Blob, BlobKey, BlobMeta,
    DeleteRequest, ExistRequest, GetOptions, GetRequest, GetSource, PutOptions, PutRequest, DeleteOptions,
};

mod prelude {
    pub use anyhow::Result;
    pub use bob_backend::core::{Backend, Operation};
    pub use bob_common::{
        bob_client::{BobClient, Factory},
        configs::node::Node as NodeConfig,
        data::{BobData, BobFlags, BobKey, BobMeta, BobOptions, DiskPath, VDiskId},
        error::Error,
        mapper::Virtual,
        metrics::{
            ALIEN_BLOBS_COUNT, AVAILABLE_NODES_COUNT, BLOBS_COUNT, CLIENT_EXIST_COUNTER,
            CLIENT_EXIST_ERROR_COUNT_COUNTER, CLIENT_EXIST_TIMER, CLIENT_GET_COUNTER,
            CLIENT_GET_ERROR_COUNT_COUNTER, CLIENT_GET_TIMER, CLIENT_PUT_COUNTER,
            CLIENT_PUT_ERROR_COUNT_COUNTER, CLIENT_PUT_TIMER, GRINDER_EXIST_COUNTER,
            GRINDER_EXIST_ERROR_COUNT_COUNTER, GRINDER_EXIST_TIMER, GRINDER_GET_COUNTER,
            GRINDER_GET_ERROR_COUNT_COUNTER, GRINDER_GET_TIMER, GRINDER_PUT_COUNTER,
            GRINDER_PUT_ERROR_COUNT_COUNTER, GRINDER_PUT_TIMER, INDEX_MEMORY,
        },
        node::{Node, Output as NodeOutput},
    };
    pub use bob_grpc::{
        bob_api_server::BobApi, Blob, BlobMeta, DeleteOptions, DeleteRequest, ExistRequest,
        ExistResponse, GetOptions, GetRequest, Null, OpStatus, PutOptions, PutRequest,
    };
    pub use futures::{future, stream::FuturesUnordered, Future, FutureExt, StreamExt};
    pub use std::{
        collections::HashMap,
        fmt::{Debug, Formatter, Result as FmtResult},
        io::Write,
        pin::Pin,
        sync::Arc,
        time::{Duration, Instant},
    };
    pub use stopwatch::Stopwatch;
    pub use tokio::{
        task::{JoinError, JoinHandle},
        time::interval,
    };
    pub use tonic::{Code, Request, Response, Status};
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) mod test_utils {
    use bob_common::{
        bob_client::{GetResult, PingResult, PutResult},
        data::BobMeta,
    };
    use chrono::Local;
    use env_logger::fmt::{Color, Formatter as EnvFormatter};
    use log::{Level, Record};
    use std::io::Result as IOResult;

    use crate::prelude::*;

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
        let inner = BobData::new(vec![].into(), BobMeta::new(timestamp));
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
