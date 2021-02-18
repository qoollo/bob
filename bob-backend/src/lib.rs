mod core;
mod mem_backend;
mod pearl;
mod stub_backend;

#[cfg(test)]
mod mem_tests;

pub(crate) use self::{
    core::{Backend, Operation},
    mem_backend::MemBackend,
    pearl::{init_pearl, Group, Holder, Pearl},
    stub_backend::StubBackend,
};
pub(crate) use super::prelude::*;

mod prelude {
    pub(crate) use super::*;

    pub(crate) use super::core::BackendStorage;
    pub(crate) use configs::BackendType;
    pub(crate) use data::{BobMeta, BobOptions};
    pub(crate) use mapper::Virtual;
    pub(crate) use tokio::sync::RwLock;
}
