use super::prelude::*;

macro_rules! sized_key {
    ($t:ident, $n:expr) => {
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub struct $t(Vec<u8>);

        impl<T: Into<Vec<u8>>> From<T> for $t {
            fn from(t: T) -> Self {
                let mut v = t.into();
                v.resize(Self::LEN as usize, 0);
                Self(v)
            }
        }

        impl KeyTrait for $t {
            const LEN: u16 = $n;
        }

        impl Default for $t {
            fn default() -> Self {
                Self(vec![0_u8; Self::LEN as usize])
            }
        }

        impl AsRef<[u8]> for $t {
            fn as_ref(&self) -> &[u8] {
                &self.0
            }
        }

        impl AsRef<$t> for $t {
            fn as_ref(&self) -> &$t {
                self
            }
        }

        impl PartialOrd for $t {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                use std::cmp::Ordering;
                for i in (0..($t::LEN as usize)).rev() {
                    let ord = self.0[i].cmp(&other.0[i]);
                    if ord != Ordering::Equal {
                        return Some(ord);
                    }
                }
                Some(Ordering::Equal)
            }
        }

        impl Ord for $t {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.partial_cmp(other).unwrap()
            }
        }
    };
}

sized_key!(Key1, 1);
sized_key!(Key2, 2);
sized_key!(Key4, 4);
sized_key!(Key8, 8);
sized_key!(Key16, 16);
sized_key!(Key32, 32);
