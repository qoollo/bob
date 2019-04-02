use crate::core::data::{BobKey, BobData};

use tokio::prelude::{Future};


use futures::future::ok;
#[derive(Clone)]
pub struct Backend {

}

pub struct BackendResult{

}

pub struct BackendError {

}

impl Backend {
    pub fn put(&self, _key: BobKey, _data: BobData) -> impl Future<Item = BackendResult, Error = BackendError> {
        ok(BackendResult{})
    }
}