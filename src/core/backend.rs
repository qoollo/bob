use crate::core::data::{BobData, BobKey};

use tokio::prelude::Future;

use futures::future::ok;
#[derive(Clone)]
pub struct Backend {}

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

impl Backend {
    pub fn put(
        &self,
        key: BobKey,
        _data: BobData,
    ) -> impl Future<Item = BackendResult, Error = BackendError> {
        debug!("PUT[{}]: hi from backend", key);
        ok(BackendResult {})
    }
    pub fn get(&self, key: BobKey) -> impl Future<Item = BackendGetResult, Error = BackendError> {
        debug!("GET[{}]: hi from backend", key);
        ok(BackendGetResult { data: vec![0] })
    }
}
