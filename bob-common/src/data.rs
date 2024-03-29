use crate::{
    error::Error,
};
use bytes::{Bytes, BytesMut};
use std::{
    convert::TryInto,
    fmt::{Debug, Formatter, Result as FmtResult},
    hash::Hash,
};

include!(concat!(env!("OUT_DIR"), "/key_constants.rs"));

#[derive(Copy, Clone, Hash, PartialEq, Eq, Debug)]
pub struct BobKey([u8; BOB_KEY_SIZE]);

impl From<u64> for BobKey {
    fn from(n: u64) -> Self {
        let mut key = [0; BOB_KEY_SIZE];
        key.iter_mut().zip(n.to_le_bytes()).for_each(|(a, b)| {
            *a = b;
        });
        Self(key)
    }
}

impl<'a> From<&'a [u8]> for BobKey {
    fn from(a: &[u8]) -> Self {
        let data = a.try_into().expect("key size mismatch");
        Self(data)
    }
}

impl From<Vec<u8>> for BobKey {
    fn from(v: Vec<u8>) -> Self {
        let data = v.try_into().expect("key size mismatch");
        Self(data)
    }
}

impl From<BobKey> for Vec<u8> {
    fn from(val: BobKey) -> Self {
        val.iter().cloned().collect()
    }
}

impl From<BobKey> for [u8; BOB_KEY_SIZE] {
    fn from(val: BobKey) -> Self {
        val.0
    }
}

impl BobKey {
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &u8> {
        self.0.iter()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Display for BobKey {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        for key in self.iter() {
            write!(f, "{:02X}", key)?;
        }
        Ok(())
    }
}

impl std::str::FromStr for BobKey {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, ()> {
        let mut data = [0; BOB_KEY_SIZE];
        for i in (0..s.len()).step_by(2) {
            if let Ok(n) = u8::from_str_radix(&s[i..i + 2], 16) {
                data[i / 2] = n
            }
        }
        Ok(Self(data))
    }
}

impl Default for BobKey {
    fn default() -> Self {
        BobKey([0; BOB_KEY_SIZE])
    }
}


#[derive(Clone)]
pub struct BobData {
    inner: Bytes,
    meta: BobMeta,
}

impl BobData {
    const TIMESTAMP_LEN: usize = 8;

    pub fn new(inner: Bytes, meta: BobMeta) -> Self {
        BobData { inner, meta }
    }

    pub fn inner(&self) -> &[u8] {
        &self.inner
    }

    pub fn into_inner(self) -> Bytes {
        self.inner
    }

    pub fn meta(&self) -> &BobMeta {
        &self.meta
    }

    pub fn to_serialized_bytes(&self) -> Bytes {
        let mut result = BytesMut::with_capacity(Self::TIMESTAMP_LEN + self.inner.len());
        result.extend_from_slice(&self.meta.timestamp.to_be_bytes());
        result.extend_from_slice(&self.inner);
        result.freeze()
    }

    pub fn from_serialized_bytes(mut bob_data: Bytes) -> Result<BobData, Error> {
        let ts_bytes = bob_data.split_to(Self::TIMESTAMP_LEN);
        let ts_bytes = (&*ts_bytes)
            .try_into()
            .map_err(|e| Error::storage(format!("parse error: {}", e)))?;
        let timestamp = u64::from_be_bytes(ts_bytes);
        let meta = BobMeta::new(timestamp);
        Ok(BobData::new(bob_data, meta))
    }
}

impl Debug for BobData {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        f.debug_struct("BobData")
            .field("len", &self.inner.len())
            .field("meta", self.meta())
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct BobMeta {
    timestamp: u64,
}
impl BobMeta {
    pub fn new(timestamp: u64) -> Self {
        Self { timestamp }
    }

    #[inline]
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn stub() -> Self {
        BobMeta { timestamp: 1 }
    }
}
