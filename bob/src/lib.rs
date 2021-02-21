#![type_length_limit = "3245934"]
#![feature(proc_macro_hygiene, decl_macro, drain_filter)]
#![warn(clippy::pedantic)]
#![allow(clippy::used_underscore_binding)]
#![warn(missing_debug_implementations)]
// #![warn(missing_docs)]

//! Library requires tokio runtime.

#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate rocket;
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate metrics;

pub mod api;
pub mod core;

pub use crate::core::{grinder::Grinder, server::Server as BobServer};
pub use bob_common::{
    bob_client::Factory,
    configs::cluster::{
        Cluster as ClusterConfig, Node as ClusterNodeConfig, Replica as ReplicaConfig,
        VDisk as VDiskConfig,
    },
    mapper::Virtual as VirtualMapper,
    metrics::init_counters,
};
pub use bob_grpc::{
    bob_api_client::BobApiClient, bob_api_server::BobApiServer, Blob, BlobKey, BlobMeta,
    ExistRequest, GetOptions, GetRequest, GetSource, PutOptions, PutRequest,
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
        bob_api_server::BobApi, Blob, BlobMeta, ExistRequest, ExistResponse, GetOptions,
        GetRequest, Null, OpStatus, PutOptions, PutRequest,
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
