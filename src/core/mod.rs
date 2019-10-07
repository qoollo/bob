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
    BackendGetResult, BackendOperation, BackendPutResult, Get, GetResult, Put, PutResult,
};

mod prelude {
    pub(crate) use super::configs::node::NodeConfig;
    pub(crate) use super::data::{print_vec, BobData, BobKey, ClusterResult};
    pub(crate) use super::*;
    pub(crate) use super::{link_manager::LinkManager, mapper::VDiskMapper};
    pub(crate) use backend::{Backend, Error as BackendError};
    pub(crate) use data::Node;
    pub(crate) use futures::future;
    pub(crate) use std::sync::Arc;
}
