use crate::core::backend::*;
use crate::core::data::{BackendOperation, BobData, BobKey, BobMeta};
use futures::future::ok;

#[derive(Clone)]
pub struct StubBackend {}

impl Backend for StubBackend {
    fn put(&self, _op: &BackendOperation, key: BobKey, data: BobData) -> BackendPutFuture {
        debug!("PUT[{}]: hi from backend, timestamp: {}", key, data.meta);
        Box::new(ok(BackendResult {}))
    }
    fn get(&self, _op: &BackendOperation, key: BobKey) -> BackendGetFuture {
        debug!("GET[{}]: hi from backend", key);
        Box::new(ok(BackendGetResult {
            data: BobData { data: vec![0], meta: BobMeta::new_stub() },
        }))
    }
}
