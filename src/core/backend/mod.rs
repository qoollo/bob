pub mod stub_backend;
use crate::core::data::{BobData, BobKey};
use tokio::prelude::Future;

#[derive(Debug)]
pub struct BackendResult {}

#[derive(Debug)]
pub enum BackendError {
    NotFound,
    __Nonexhaustive,
}

#[derive(Debug)]
pub struct BackendGetResult {
    pub data: Vec<u8>,
}
pub type BackendPutFuture = Box<Future<Item = BackendResult, Error = BackendError> + Send>;
pub type BackendGetFuture = Box<Future<Item = BackendGetResult, Error = BackendError> + Send>;
pub trait Backend {
    fn put(&self, key: BobKey, _data: BobData) -> BackendPutFuture;
    fn get(&self, key: BobKey) -> BackendGetFuture;
}
