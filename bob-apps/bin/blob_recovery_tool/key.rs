use super::prelude::*;

fn le_cmp_keys<const N: usize>(x: &[u8], y: &[u8]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for i in (0..N).rev() {
        let ord = x[i].cmp(&y[i]);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

macro_rules! sized_key {
    ($t:ident, $r:ident, $n:expr) => {
        #[derive(PartialEq, Eq)]
        pub struct $r<'a>(&'a [u8]);

        impl<'a> RefKeyTrait<'a> for $r<'a> {}

        impl<'a> From<&'a [u8]> for $r<'a> {
            fn from(v: &'a [u8]) -> Self {
                Self(v)
            }
        }

        impl<'a> PartialOrd for $r<'a> {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(le_cmp_keys::<$n>(self.0, other.0))
            }
        }

        impl<'a> Ord for $r<'a> {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.partial_cmp(other).unwrap()
            }
        }

        #[derive(Clone, Debug, PartialEq, Eq)]
        pub struct $t(Vec<u8>);

        impl<T: Into<Vec<u8>>> From<T> for $t {
            fn from(t: T) -> Self {
                let mut v = t.into();
                v.resize(Self::LEN as usize, 0);
                Self(v)
            }
        }

        impl<'a> KeyTrait<'a> for $t {
            type Ref = $r<'a>;
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
                Some(le_cmp_keys::<$n>(&self.0, &other.0))
            }
        }

        impl Ord for $t {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.partial_cmp(other).unwrap()
            }
        }
    };
}

sized_key!(Key4, RefKey4, 4);
sized_key!(Key8, RefKey8, 8);
sized_key!(Key16, RefKey16, 16);
sized_key!(Key32, RefKey32, 32);
