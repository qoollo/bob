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

    #[inline]
    pub fn read(&self, key: Key) -> impl Future<Output = Result<Vec<u8>>> + '_ {
        self.storage.as_ref().unwrap().read(key)
    }

    #[inline]
    pub fn write(&self, key: Key, value: Vec<u8>) -> impl Future<Output = Result<()>> + '_ {
        self.storage.as_ref().unwrap().write(key, value)
    }

    #[inline]
    pub fn contains(&self, key: Key) -> impl Future<Output = Result<bool>> + '_ {
        self.storage.as_ref().unwrap().contains(key)
    }

    #[inline]
    pub fn records_count(&self) -> impl Future<Output = usize> + '_ {
        self.storage.as_ref().unwrap().records_count()
    }

    #[inline]
    pub fn blobs_count(&self) -> impl Future<Output = usize> + '_ {
        self.storage.as_ref().unwrap().blobs_count()
    }

    #[inline]
    pub fn records_count_in_active_blob(&self) -> impl Future<Output = Option<usize>> + '_ {
        self.storage
            .as_ref()
            .unwrap()
            .records_count_in_active_blob()
    }

    #[inline]
    pub fn close_active_blob(&self) -> impl Future<Output = ()> + '_ {
        self.storage.as_ref().unwrap().close_active_blob()
    }

    #[inline]
    pub fn close(&self) -> impl Future<Output = Result<()>> {
        self.storage.clone().unwrap().close()
    }

    #[inline]
    pub fn set(&mut self, storage: PearlStorage) -> Option<PearlStorage> {
        self.storage.replace(storage)
    }

    pub fn index_memory(&self) -> impl Future<Output = usize> + '_ {
        self.storage.as_ref().unwrap().index_memory()
    }
}
