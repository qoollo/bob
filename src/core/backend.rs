use crate::core::data::{BobKey, BobData};

use tokio::prelude::{Future};


use futures::future::ok;
pub struct Backend {

}

pub struct BackendError {

}

impl Backend {
    pub fn put(&self, _key: BobKey, _data: BobData) -> impl Future<Item = (), Error = BackendError> {
        ok(())
    }
}