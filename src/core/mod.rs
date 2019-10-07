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
    pub(crate) use super::configs::NodeConfig;
    pub(crate) use super::data::{
        print_vec, BobData, BobKey, BobMeta, ClusterResult, NodeDisk, VDisk, VDiskId,
    };
    pub(crate) use super::metrics::{BobClientMetrics, MetricsContainerBuilder};
    pub(crate) use super::*;
    pub(crate) use super::{link_manager::LinkManager, mapper::VDiskMapper};
    pub(crate) use crate::api::grpc::client::BobApiClient;
    pub(crate) use crate::api::grpc::{
        Blob, BlobKey, BlobMeta, GetOptions, GetRequest, Null, PutOptions, PutRequest,
    };
    pub(crate) use backend::{Backend, Error as BackendError};
    pub(crate) use data::Node;
    pub(crate) use futures::{future, Future};
    pub(crate) use std::collections::HashMap;
    pub(crate) use std::pin::Pin;
    pub(crate) use std::sync::Arc;
    pub(crate) use std::time::Duration;
    pub(crate) use tokio::runtime::TaskExecutor;
}
