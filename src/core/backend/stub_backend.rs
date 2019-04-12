use crate::core::backend::*;
use crate::core::data::{BobData, BobKey};
use tokio::prelude::Future;

use futures::future::ok;
#[derive(Clone)]
pub struct StubBackend {}

impl Backend for StubBackend {
    fn put(&self, key: BobKey, _data: BobData) -> BackendPutFuture {
        debug!("PUT[{}]: hi from backend", key);
        Box::new(ok(BackendResult {}))
    }
    fn get(&self, key: BobKey) -> BackendGetFuture {
        debug!("GET[{}]: hi from backend", key);
        Box::new(ok(BackendGetResult { data: vec![0] }))
    }
}
