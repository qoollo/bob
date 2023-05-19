use std::{
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    hash::{Hash, Hasher},
    sync::Arc, 
    marker::PhantomData,
};
use serde::{
    de::{Visitor, Deserialize, Deserializer},
    ser::{Serialize, Serializer}
};

/// Marker type to distinct different Name types between each other
pub trait NameMarker: Sized {
    /// Returns type name for Debug printing. 
    /// Example: `NodeName` for node, `DiskName` for disk
    fn display_name() -> &'static str;
}

/// Generic name struct. Clone is lightweight
pub struct Name<TMarker: NameMarker> {
    str: Arc<str>,
    _phantom: PhantomData<TMarker>
}

impl<TMarker: NameMarker> Name<TMarker> {
    pub fn new(val: &str) -> Self {
        Self {
            str: val.into(),
            _phantom: PhantomData::default()
        }
    }
    pub fn as_str(&self) -> &str {
        self.str.as_ref()
    }
    pub fn to_string(&self) -> String {
        String::from(self.str.as_ref())
    }
}

impl<TMarker: NameMarker> Clone for Name<TMarker> {
    fn clone(&self) -> Self {
        Self {
            str: self.str.clone(),
            _phantom: PhantomData::default()
        }
    }
}

impl<TMarker: NameMarker> From<&str> for Name<TMarker> {
    fn from(val: &str) -> Self {
        Self {
            str: val.into(),
            _phantom: PhantomData::default()
        }
    }
}

impl<TMarker: NameMarker> From<&String> for Name<TMarker> {
    fn from(val: &String) -> Self {
        Self {
            str: val.as_str().into(),
            _phantom: PhantomData::default()
        }
    }
}

impl<TMarker: NameMarker> From<&Name<TMarker>> for Name<TMarker> {
    fn from(val: &Name<TMarker>) -> Self {
        Self {
            str: val.str.clone(),
            _phantom: PhantomData::default()
        }
    }
}

impl<TMarker: NameMarker> AsRef<str> for Name<TMarker> {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl<TMarker: NameMarker> AsRef<[u8]> for Name<TMarker> {
    fn as_ref(&self) -> &[u8] {
        self.as_str().as_bytes()
    }
}

macro_rules! impl_str_partial_eq {
    ($ty:ty, $other:ty) => {
        impl<'a, TMarker: NameMarker> PartialEq<$other> for $ty {
            fn eq(&self, other: &$other) -> bool {
                PartialEq::eq(AsRef::<str>::as_ref(self), AsRef::<str>::as_ref(other))
            }
        }
    };
}

impl_str_partial_eq!(Name<TMarker>, Name<TMarker>);
impl_str_partial_eq!(Name<TMarker>, str);
impl_str_partial_eq!(str, Name<TMarker>);
impl_str_partial_eq!(Name<TMarker>, &'a str);
impl_str_partial_eq!(&'a str, Name<TMarker>);
impl_str_partial_eq!(Name<TMarker>, String);
impl_str_partial_eq!(String, Name<TMarker>);

impl<TMarker: NameMarker> Eq for Name<TMarker> { }


impl<TMarker: NameMarker> Hash for Name<TMarker> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state)
    }
}

impl<TMarker: NameMarker> Debug for Name<TMarker> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple(TMarker::display_name()).field(&self.as_str()).finish()
    }
}

impl<TMarker: NameMarker> Display for Name<TMarker> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str(self.as_str())
    }
}

// ==== Serialization/deserialization ===

/// Visistor for deserialization
struct ArcStrVisitor;

impl<'de> Visitor<'de> for ArcStrVisitor {
    type Value = Arc<str>;

    fn expecting(&self, formatter: &mut Formatter) -> FmtResult {
        formatter.write_str("string")
    }
    
    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error, 
    {
        Ok(v.into())
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: serde::de::Error, 
    {
        Ok(v.into())
    }
}

impl<'de, TMarker: NameMarker> Deserialize<'de> for Name<TMarker> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {   
        deserializer.deserialize_str(ArcStrVisitor).map(|v| Self { str: v, _phantom: PhantomData::default() })
    }
}

impl<TMarker: NameMarker> Serialize for Name<TMarker> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}