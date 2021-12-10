use crate::prelude::*;

include!(concat!(env!("OUT_DIR"), "/key_constants.rs"));

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Key(Vec<u8>);

#[derive(PartialEq, Eq)]
pub struct RefKey<'a>(&'a [u8]);

impl<'a> RefKeyTrait<'a> for RefKey<'a> {}

impl<T: Into<Vec<u8>>> From<T> for Key {
    fn from(t: T) -> Self {
        let mut v = t.into();
        v.resize(Self::LEN as usize, 0);
        Self(v)
    }
}

impl<'a> KeyTrait<'a> for Key {
    const LEN: u16 = BOB_KEY_SIZE;

    type Ref = RefKey<'a>;
}

impl Default for Key {
    fn default() -> Self {
        Self(vec![0_u8; Self::LEN as usize])
    }
}

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<Key> for Key {
    fn as_ref(&self) -> &Key {
        self
    }
}

impl<'a> From<&'a [u8]> for RefKey<'a> {
    fn from(v: &'a [u8]) -> Self {
        Self(v)
    }
}

impl<'a> PartialOrd for RefKey<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;
        for i in (0..(Key::LEN as usize)).rev() {
            let ord = self.0[i].cmp(&other.0[i]);
            if ord != Ordering::Equal {
                return Some(ord);
            }
        }
        Some(Ordering::Equal)
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.as_ref_key().partial_cmp(&other.as_ref_key())
    }
}

impl<'a> Ord for RefKey<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_ref_key().cmp(&other.as_ref_key())
    }
}

pub struct Data {
    data: Vec<u8>,
    timestamp: u64,
}

impl Data {
    const TIMESTAMP_LEN: usize = 8;

    pub fn to_vec(&self) -> Vec<u8> {
        let mut result = self.timestamp.to_be_bytes().to_vec();
        result.extend_from_slice(&self.data);
        result
    }

    pub fn from_bytes(data: &[u8]) -> Result<BobData, Error> {
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
