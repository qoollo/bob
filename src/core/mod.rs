pub mod backend;
pub mod bob_client;
pub mod cluster;
pub mod configs;
pub mod data;
pub mod grinder;
pub mod link_manager;
pub mod mapper;
pub mod metrics;
pub mod server;

pub(crate) use super::prelude::*;
pub(crate) use backend::{
    init_pearl, Backend, BackendExistResult, BackendGetResult, BackendOperation, BackendPingResult,
    BackendPutResult, Error as BackendError, Exist as BackendExist, Get as BackendGet,
    Put as BackendPut,
};

mod prelude {
    pub(crate) use super::*;

    pub(crate) use bob_client::{BobClient, Factory, GetResult as BobClientGetResult};
    pub(crate) use cluster::{get_cluster, Cluster};
    pub(crate) use configs::{
        ClusterConfig, DiskPath as ConfigDiskPath, Node as ClusterNodeConfig, NodeConfig,
    };
    pub(crate) use data::{
        BobData, BobFlags, BobKey, BobMeta, BobOptions, DiskPath, Node, NodeOutput, VDisk, VDiskId,
    };
    pub(crate) use dipstick::{
        AtomicBucket, Counter, Graphite, InputKind, InputScope, MetricName, MetricValue, Output,
        Prefixed, Proxy, ScheduleFlush, ScoreType, TimeHandle, Timer, Void,
    };
    pub(crate) use futures::{
        future, stream::FuturesUnordered, Future, FutureExt, StreamExt, TryFutureExt,
    };
    pub(crate) use grinder::Grinder;
    pub(crate) use grpc::{
        bob_api_server::BobApi, ExistRequest, ExistResponse, GetOptions, GetSource, Null, OpStatus,
        PutOptions,
    };
    pub(crate) use link_manager::LinkManager;
    pub(crate) use mapper::Virtual;
    pub(crate) use metrics::{
        BobClientMetrics, MetricsContainerBuilder, CLIENT_GET_COUNTER,
        CLIENT_GET_ERROR_COUNT_COUNTER, CLIENT_GET_TIMER, CLIENT_PUT_COUNTER,
        CLIENT_PUT_ERROR_COUNT_COUNTER, CLIENT_PUT_TIMER, GRINDER_GET_COUNTER,
        GRINDER_GET_ERROR_COUNT_COUNTER, GRINDER_GET_TIMER, GRINDER_PUT_COUNTER,
        GRINDER_PUT_ERROR_COUNT_COUNTER, GRINDER_PUT_TIMER,
    };
    pub(crate) use stopwatch::Stopwatch;
    pub(crate) use tokio::time::{interval, timeout};
    pub(crate) use tonic::{
        transport::{Channel, Endpoint},
        Code, Request, Response, Status,
    };
}

#[cfg(test)]
pub(crate) mod test_utils {
    use super::prelude::*;
    use super::{
        backend::{BackendGetResult, BackendPingResult, BackendPutResult},
        bob_client::{Get, PingResult, PutResult},
        data::{BobData, BobMeta, Node, NodeOutput},
        BackendError,
    };
    use futures::future::ready;

    pub(crate) fn ping_ok(node_name: String) -> PingResult {
        Ok(NodeOutput::new(node_name, BackendPingResult {}))
    }

    pub(crate) fn ping_err(node_name: String) -> PingResult {
        Err(NodeOutput::new(node_name, BackendError::Internal))
    }

    pub(crate) fn put_ok(node_name: String) -> PutResult {
        Ok(NodeOutput::new(node_name, BackendPutResult {}))
    }

    pub(crate) fn put_err(node_name: String) -> PutResult {
        Err(NodeOutput::new(node_name, BackendError::Internal))
    }

    pub(crate) fn get_ok(node_name: String, timestamp: i64) -> Get {
        Get({
            ready(Ok(NodeOutput::new(
                node_name,
                BackendGetResult {
                    data: BobData::new(vec![], BobMeta::new(timestamp)),
                },
            )))
            .boxed()
        })
    }

    pub(crate) fn get_err(node_name: String) -> Get {
        Get({ ready(Err(NodeOutput::new(node_name, BackendError::Internal))).boxed() })
    }
}
