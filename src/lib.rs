#![crate_type = "lib"]
#![allow(clippy::needless_lifetimes)]
#![feature(async_closure)]

#[macro_use]
extern crate bitflags;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate cfg_if;
#[macro_use]
extern crate dipstick;

mod api;
mod core_inner;

pub use self::api::grpc;
pub use self::core_inner::{
    backend, bob_client as client, configs, grinder, mapper, metrics, server,
};

mod prelude {
    pub(crate) use super::*;
    pub(crate) use grpc::{client::BobApiClient, Blob, BlobKey, BlobMeta, GetRequest, PutRequest};
    // pub(crate) use futures::{future, task::Spawn, Future};
    // pub(crate) use futures_locks::RwLock;
    // pub(crate) use std::io::ErrorKind;
    // pub(crate) use std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration};
    // pub(crate) use stopwatch::Stopwatch;
    // pub(crate) use tokio::timer::Error as TimerError;
    // pub(crate) use tonic::{Code, Status};
    // pub(crate) use tonic::{Request, Response};
}
