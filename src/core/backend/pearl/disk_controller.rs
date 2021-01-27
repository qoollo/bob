use crate::prelude::Result;
use futures::Future;
use pearl::{Error as PearlError, ErrorKind as PearlErrorKind};

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

    pub async fn write(&self, key: Key, value: Vec<u8>) -> Result<()> {
        if let Some(storage) = self.storage.as_ref() {
            match storage.write(key, value).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    if let Some(pearl_err) = e.downcast_ref::<PearlError>() {
                        error!("pearl error: {:#}", pearl_err);
                        match pearl_err.kind() {
                            PearlErrorKind::WorkDirUnavailable(msg) => {
                                todo!("disable access to pearl, wait for work dir to become available again");
                            }
                            _ => {
                                todo!("return error as is");
                            }
                        }
                    } else {
                        error!("not pearl error: {:#}", e);
                        todo!()
                    }
                    Err(e)
                }
            }
        } else {
            todo!("need implementation for the case when storage was not set");
        }
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

#[tokio::test]
async fn test_disk_controller_write() {
    let mut disk_controller = DiskController::new();
    let mut storage = pearl::Builder::new()
        .work_dir("some/irrelevant/path/")
        .blob_file_name_prefix("test")
        .max_blob_size(10_000)
        .max_data_in_blob(10_000)
        .build()
        .unwrap();
    storage.init().await.unwrap();
    disk_controller.set(storage);
    let key = Key::from(13);
    let data = vec![1, 2, 3, 4];
    disk_controller.write(key, data).await.unwrap();
    todo!()
}
