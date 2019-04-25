use crate::core::backend::*;
use crate::core::data::{BobData, BobKey, WriteOption};
use futures::future::ok;

#[derive(Clone)]
pub struct StubBackend {}

impl Backend for StubBackend {
    fn put(&self, _op: &WriteOption, key: BobKey, _data: BobData) -> BackendPutFuture {
        debug!("PUT[{}]: hi from backend", key);
        Box::new(ok(BackendResult {}))
    }
    fn get(&self, _op: &WriteOption, key: BobKey) -> BackendGetFuture {
        debug!("GET[{}]: hi from backend", key);
        Box::new(ok(BackendGetResult {
            data: BobData { data: vec![0] },
        }))
    }
}
