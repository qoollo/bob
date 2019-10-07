mod core;
mod error;
mod mem_backend;
mod pearl;
mod stub_backend;

#[cfg(test)]
mod mem_tests;

pub(crate) use self::core::{
    Backend, BackendGetResult, BackendOperation, BackendPutResult, Get, GetResult, Put, PutResult,
};
pub(crate) use self::error::Error;
pub(crate) use self::mem_backend::MemBackend;
pub(crate) use self::pearl::PearlBackend;
pub(crate) use self::stub_backend::StubBackend;
pub(crate) use super::prelude::*;

mod prelude {
    pub(crate) use super::core::{BackendStorage, RunResult};
    pub(crate) use super::*;
    pub(crate) use super::{MemBackend, PearlBackend, StubBackend};
    pub(crate) use crate::core::configs::node::{BackendType, DiskPath};
    pub(crate) use crate::core::data::VDiskId;
    pub(crate) use crate::core::data::{BobMeta, BobOptions};
    pub(crate) use crate::core::mapper::VDiskMapper;
    pub(crate) use futures::{task::Spawn, Future};
    pub(crate) use futures_locks::RwLock;
    pub(crate) use std::{collections::HashMap, io::ErrorKind, pin::Pin};
    pub(crate) use tokio::timer::Error as TimerError;
    pub(crate) use tonic::{Code, Status};
}
