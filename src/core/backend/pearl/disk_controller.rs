use crate::prelude::Result;
use futures::Future;

use super::{prelude::Key, PearlStorage};

#[derive(Debug, Clone)]
pub struct DiskController {
    storage: Option<PearlStorage>,
}

impl DiskController {
    pub fn new() -> Self {
        Self { storage: None }
    }

    pub fn write(&self, key: Key, value: Vec<u8>) -> impl Future<Output = Result<()>> + '_ {
        self.storage.as_ref().unwrap().write(key, value)
    }

    pub fn close_active_blob(&self) -> impl Future<Output = ()> + '_ {
        self.storage.as_ref().unwrap().close_active_blob()
    }

    pub fn close(&self) -> impl Future<Output = Result<()>> {
        self.storage.clone().unwrap().close()
    }

    #[deprecated]
    pub fn storage(&self) -> &PearlStorage {
        self.storage.as_ref().unwrap()
    }

    pub fn set(&mut self, storage: PearlStorage) -> Option<PearlStorage> {
        self.storage.replace(storage)
    }
}
