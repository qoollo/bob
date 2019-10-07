mod backend;
mod bob_client;
mod cluster;
mod configs;
mod data;
mod grinder;
mod link_manager;
mod mapper;
mod metrics;
mod server;

pub(crate) use backend::{
    BackendGetResult, BackendOperation, BackendPingResult, BackendPutResult, Get, GetResult, Put,
};

mod prelude {
    pub(crate) use super::bob_client::{BobClient, BobClientFactory};
    pub(crate) use super::cluster::{get_cluster, Cluster};
    pub(crate) use super::configs::{Node as ClusterNodeConfig, NodeConfig};
    pub(crate) use super::data::{
        print_vec, BobData, BobFlags, BobKey, BobMeta, BobOptions, ClusterResult, NodeDisk, VDisk,
        VDiskId,
    };
    pub(crate) use super::metrics::{
        BobClientMetrics, MetricsContainerBuilder, CLIENT_GET_COUNTER,
        CLIENT_GET_ERROR_COUNT_COUNTER, CLIENT_GET_TIMER, CLIENT_PUT_COUNTER, CLIENT_PUT_TIMER,
        GRINDER_GET_COUNTER, GRINDER_GET_ERROR_COUNT_COUNTER, GRINDER_GET_TIMER,
        GRINDER_PUT_COUNTER, GRINDER_PUT_ERROR_COUNT_COUNTER, GRINDER_PUT_TIMER,
    };
    pub(crate) use super::*;
    pub(crate) use super::{link_manager::LinkManager, mapper::VDiskMapper};
    pub(crate) use crate::api::grpc::client::BobApiClient;
    pub(crate) use crate::api::grpc::{
        Blob, BlobKey, BlobMeta, GetOptions, GetRequest, GetSource, Null, PutOptions, PutRequest,
    };
    pub(crate) use backend::{Backend, Error as BackendError};
    pub(crate) use data::Node;
    pub(crate) use futures::{future, task::Spawn, Future};
    pub(crate) use std::collections::HashMap;
    pub(crate) use std::pin::Pin;
    pub(crate) use std::sync::Arc;
    pub(crate) use std::sync::Mutex;
    pub(crate) use std::time::Duration;
    pub(crate) use tokio::runtime::TaskExecutor;
}
