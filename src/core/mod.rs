/// Component responsible for working with I/O.
pub mod backend;
/// GRPC client to deal with backend.
pub mod bob_client;
pub(crate) mod cluster;
/// Configuration tools.
pub mod configs;
pub(crate) mod data;
/// Component to manage cluster I/O and connections.
pub mod grinder;
pub(crate) mod link_manager;
/// Component to map storage space on disks.
pub mod mapper;
/// Tools for tracking bob different indicators.
pub mod metrics;
/// GRPC server to receive and process requests from clients.
pub mod server;

pub(crate) use super::prelude::*;
pub(crate) use backend::{
    init_pearl, Backend, BackendOperation, Error as BackendError, ExistResult, GetResult, PutResult,
};

mod prelude {
    pub(crate) use super::*;

    pub(crate) use bob_client::{BobClient, Factory, GetResult as BobClientGetResult};
    pub(crate) use cluster::{get_cluster, Cluster};
    pub(crate) use configs::{Cluster as ClusterConfig, Node as NodeConfig};
    pub(crate) use data::{
        BobData, BobFlags, BobKey, BobMeta, BobOptions, DiskPath, Node, NodeOutput, VDisk, VDiskId,
    };
    pub(crate) use dipstick::{
        AtomicBucket, Counter, Graphite, InputKind, InputScope, MetricName, MetricValue, Output,
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
    pub(crate) use stopwatch::Stopwatch;
    pub(crate) use termion::color;
    pub(crate) use tokio::{
        net::lookup_host,
        time::{interval, timeout},
    };
    pub(crate) use tonic::{
        transport::{Channel, Endpoint},
        Code, Request, Response, Status,
    };
}

#[cfg(test)]
pub(crate) mod test_utils {
    use super::bob_client::{GetResult, PingResult, PutResult};
    use super::prelude::*;

    pub(crate) fn ping_ok(node_name: String) -> PingResult {
        Ok(NodeOutput::new(node_name, ()))
    }

    pub(crate) fn put_ok(node_name: String) -> PutResult {
        Ok(NodeOutput::new(node_name, ()))
    }

    pub(crate) fn put_err(node_name: String) -> PutResult {
        Err(NodeOutput::new(node_name, BackendError::Internal))
    }

    pub(crate) fn get_ok(node_name: String, timestamp: u64) -> GetResult {
        let inner = BobData::new(vec![], BobMeta::new(timestamp));
        Ok(NodeOutput::new(node_name, inner))
    }

    pub(crate) fn get_err(node_name: String) -> GetResult {
        Err(NodeOutput::new(node_name, BackendError::Internal))
    }
}
