use crate::prelude::*;

include!(concat!(env!("OUT_DIR"), "/key_constants.rs"));

const BOB_KEY_SIZE_USIZE: usize = BOB_KEY_SIZE as usize;

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
    /// Length of the key in bytes
    const LEN: u16 = BOB_KEY_SIZE;
    /// Size of the key in memory
    const MEM_SIZE: usize = std::mem::size_of::<Vec<u8>>() + BOB_KEY_SIZE_USIZE;

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

pub fn le_cmp_keys(x: &[u8], y: &[u8]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    if BOB_KEY_SIZE_USIZE == std::mem::size_of::<usize>() {
        let x_part: usize = usize::from_le_bytes(x.try_into().unwrap());
        let y_part: usize = usize::from_le_bytes(y.try_into().unwrap());
        return x_part.cmp(&y_part);
    } else if BOB_KEY_SIZE_USIZE % std::mem::size_of::<usize>() == 0 {
        let len = BOB_KEY_SIZE_USIZE / std::mem::size_of::<usize>();
        for i in (0..len).rev() {
            let x_part: usize = usize::from_le_bytes(
                x[i * std::mem::size_of::<usize>()..(i + 1) * std::mem::size_of::<usize>()]
                    .try_into()
                    .unwrap(),
            );
            let y_part: usize = usize::from_le_bytes(
                y[i * std::mem::size_of::<usize>()..(i + 1) * std::mem::size_of::<usize>()]
                    .try_into()
                    .unwrap(),
            );
            let ord = x_part.cmp(&y_part);
            if ord != Ordering::Equal {
                return ord;
            }
        }
    } else {
        for i in (0..BOB_KEY_SIZE_USIZE).rev() {
            let ord = x[i].cmp(&y[i]);
            if ord != Ordering::Equal {
                return ord;
            }
        }
    }
    Ordering::Equal
}

impl<'a> PartialOrd for RefKey<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(le_cmp_keys(self.0, other.0))
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(le_cmp_keys(&self.0, &other.0))
    }
}

impl<'a> Ord for RefKey<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(&other).unwrap()
    }
}
