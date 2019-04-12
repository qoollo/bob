pub mod mem_backend;
pub mod stub_backend;
use crate::core::data::{BobData, BobKey};
use tokio::prelude::Future;

#[derive(Debug)]
pub struct BackendResult {}

#[derive(Debug)]
pub enum BackendError {
    NotFound,
    Other,
    __Nonexhaustive,
}

pub struct BackendGetResult {
    pub data: BobData,
}
pub type BackendPutFuture = Box<Future<Item = BackendResult, Error = BackendError> + Send>;
pub type BackendGetFuture = Box<Future<Item = BackendGetResult, Error = BackendError> + Send>;
pub trait Backend {
    fn put(&self, disk: &String, key: BobKey, _data: BobData) -> BackendPutFuture;
    fn get(&self, disk: &String, key: BobKey) -> BackendGetFuture;
}
