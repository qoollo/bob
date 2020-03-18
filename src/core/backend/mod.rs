mod core;
mod error;
mod mem_backend;
mod pearl;
mod stub_backend;

#[cfg(test)]
mod mem_tests;

pub(crate) use self::core::{
    Backend, BackendExistResult, BackendGetResult, BackendOperation, BackendPingResult, Exist,
    ExistResult, Get, GetResult, Put, PutResult,
};
pub(crate) use self::error::Error;
pub(crate) use self::mem_backend::MemBackend;
pub(crate) use self::pearl::{init_pearl, Group, Pearl};
pub(crate) use self::stub_backend::StubBackend;
pub(crate) use super::prelude::*;

mod prelude {
    pub(crate) use super::*;

    pub(crate) use super::core::{BackendStorage, RunResult};
    pub(crate) use configs::BackendType;
    pub(crate) use data::{BobMeta, BobOptions};
    pub(crate) use mapper::Virtual;
    pub(crate) use tokio::sync::RwLock;
}
