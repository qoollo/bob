use super::prelude::*;

#[derive(Clone, Debug)]
pub(crate) struct Key(Vec<u8>);

impl From<u64> for Key {
    fn from(key: u64) -> Self {
        Self(key.to_be_bytes().to_vec())
    }
}

impl From<BobKey> for Key {
    fn from(v: BobKey) -> Self {
        Self(v.bytes().cloned().collect())
    }
}

impl KeyTrait for Key {
    const LEN: u16 = std::mem::size_of::<BobKey>() as u16;
}

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

pub(crate) struct Data {
    data: Vec<u8>,
    timestamp: u64,
}

impl Data {
    const TIMESTAMP_LEN: usize = 8;

    pub(crate) fn to_vec(&self) -> Vec<u8> {
        let mut result = self.timestamp.to_be_bytes().to_vec();
        result.extend_from_slice(&self.data);
        result
    }

    pub(crate) fn from_bytes(data: &[u8]) -> Result<BobData, Error> {
        let (ts, bob_data) = data.split_at(Self::TIMESTAMP_LEN);
        let bytes = ts
            .try_into()
            .map_err(|e| Error::storage(format!("parse error: {}", e)))?;
        let timestamp = u64::from_be_bytes(bytes);
        let meta = BobMeta::new(timestamp);
        Ok(BobData::new(bob_data.to_vec(), meta))
    }
}

impl From<BobData> for Data {
    fn from(data: BobData) -> Self {
        Self {
            timestamp: data.meta().timestamp(),
            data: data.into_inner(),
        }
    }
}
