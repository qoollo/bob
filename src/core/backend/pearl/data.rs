use crate::core::backend;
use crate::core::data::{BobData, BobKey, BobMeta};
use pearl::{Key, Storage};

use futures03::Future as Future03;
use std::{boxed::Box, convert::TryInto, pin::Pin};

pub(crate) type BackendResult<T> = Result<T, backend::Error>;
pub(crate) type Future03Result<TRet> = Pin<Box<dyn Future03<Output = BackendResult<TRet>> + Send>>;

pub(crate) type PearlStorage = Storage<PearlKey>;

#[derive(Clone)]
pub(crate) struct PearlKey {
    pub key: Vec<u8>,
}
impl PearlKey {
    pub fn new(key: BobKey) -> Self {
        PearlKey {
            key: key.key.to_be_bytes().to_vec(),
        }
    }
}
impl Key for PearlKey {
    const LEN: u16 = 8;
}
impl AsRef<[u8]> for PearlKey {
    fn as_ref(&self) -> &[u8] {
        &self.key
    }
}

pub(crate) struct PearlData {
    data: Vec<u8>,
    timestamp: u32,
}
impl PearlData {
    const TIMESTAMP_LEN: usize = 4;

    pub(crate) fn new(data: Box<BobData>) -> Self {
        PearlData {
            data: data.data,
            timestamp: data.meta.timestamp,
        }
    }

    pub(crate) fn bytes(&mut self) -> Vec<u8> {
        let mut result = self.timestamp.to_be_bytes().to_vec();
        result.append(&mut self.data);
        result
    }

    pub(crate) fn parse(data: Vec<u8>) -> Result<BobData, backend::Error> {
        let (tmp, bob_data) = data.split_at(PearlData::TIMESTAMP_LEN);
        match tmp.try_into() {
            Ok(bytes) => {
                let timestamp = u32::from_be_bytes(bytes);
                Ok(BobData::new(
                    bob_data.to_vec(),
                    BobMeta::new_value(timestamp),
                ))
            }
            Err(e) => Err(backend::Error::StorageError(format!("parse error: {}", e))),
        }
    }
}
