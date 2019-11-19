use super::prelude::*;

pub(crate) type BackendResult<T> = Result<T, Error>;
pub(crate) type Future03Result<TRet> = Pin<Box<dyn Future<Output = BackendResult<TRet>> + Send>>;

pub(crate) type PearlStorage = Storage<PearlKey>;

#[derive(Clone, Debug)]
pub(crate) struct PearlKey {
    pub key: Vec<u8>,
}

impl PearlKey {
    pub fn new(bob_key: BobKey) -> Self {
        PearlKey {
            key: bob_key.key.to_be_bytes().to_vec(),
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
    timestamp: i64,
}

impl PearlData {
    const TIMESTAMP_LEN: usize = 8;

    pub(crate) fn new(data: BobData) -> Self {
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

    pub(crate) fn parse(data: Vec<u8>) -> Result<BobData, Error> {
        let (tmp, bob_data) = data.split_at(PearlData::TIMESTAMP_LEN);
        match tmp.try_into() {
            Ok(bytes) => {
                let timestamp = i64::from_be_bytes(bytes);
                Ok(BobData::new(
                    bob_data.to_vec(),
                    BobMeta::new_value(timestamp),
                ))
            }
            Err(e) => Err(Error::StorageError(format!("parse error: {}", e))),
        }
    }
}
