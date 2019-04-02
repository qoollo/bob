use crate::core::data::{BobData, BobKey};

use tokio::prelude::Future;

use futures::future::ok;
#[derive(Clone)]
pub struct Backend {}

pub struct BackendResult {}

pub struct BackendError {}

impl Backend {
    pub fn put(
        &self,
        _key: BobKey,
        _data: BobData,
    ) -> impl Future<Item = BackendResult, Error = BackendError> {
        ok(BackendResult {})
    }
}
