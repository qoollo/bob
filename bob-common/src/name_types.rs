use std::{
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    hash::{Hash, Hasher},
    sync::Arc,
};
use serde::{
    de::{Visitor, Deserialize, Deserializer},
    ser::{Serialize, Serializer}
};

/// Node name struct. Clone is lightweight
#[derive(Clone)]
pub struct NodeName(Arc<str>);

/// Disk name struct. Clone is lightweight
#[derive(Clone)]
pub struct DiskName(Arc<str>);


macro_rules! impl_str_partial_eq {
    ($ty:ty, $other:ty) => {
        impl<'a> PartialEq<$other> for $ty {
            fn eq(&self, other: &$other) -> bool {
                PartialEq::eq(AsRef::<str>::as_ref(self), AsRef::<str>::as_ref(other))
            }
        }
    };
}

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

// ============= NodeName =============

impl NodeName {
    pub fn new(val: &str) -> Self {
        Self(val.into())
    }
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
    pub fn to_string(&self) -> String {
        String::from(self.0.as_ref())
    }
}

impl From<&str> for NodeName {
    fn from(val: &str) -> Self {
        Self(val.into())
    }
}

impl From<&String> for NodeName {
    fn from(val: &String) -> Self {
        Self(val.as_str().into())
    }
}

impl AsRef<str> for NodeName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<[u8]> for NodeName {
    fn as_ref(&self) -> &[u8] {
        self.as_str().as_bytes()
    }
}

impl_str_partial_eq!(NodeName, NodeName);
impl_str_partial_eq!(NodeName, str);
impl_str_partial_eq!(str, NodeName);
impl_str_partial_eq!(NodeName, &'a str);
impl_str_partial_eq!(&'a str, NodeName);
impl_str_partial_eq!(NodeName, String);
impl_str_partial_eq!(String, NodeName);

impl Eq for NodeName { }

impl Hash for NodeName {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state)
    }
}

impl Debug for NodeName {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("NodeName").field(&self.as_str()).finish()
    }
}

impl Display for NodeName {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for NodeName {
    fn deserialize<D>(deserializer: D) -> Result<DiskName, D::Error>
    where
        D: Deserializer<'de>,
    {   
        deserializer.deserialize_str(ArcStrVisitor).map(|v| NodeName(v))
    }
}

impl Serialize for NodeName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}


// ============= DiskName =============

impl DiskName {
    pub fn new(val: &str) -> Self {
        Self(val.into())
    }
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
    pub fn to_string(&self) -> String {
        String::from(self.0.as_ref())
    }
}

impl From<&str> for DiskName {
    fn from(val: &str) -> Self {
        Self(val.into())
    }
}

impl From<&String> for DiskName {
    fn from(val: &String) -> Self {
        Self(val.as_str().into())
    }
}

impl AsRef<str> for DiskName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<[u8]> for DiskName {
    fn as_ref(&self) -> &[u8] {
        self.as_str().as_bytes()
    }
}

impl_str_partial_eq!(DiskName, DiskName);
impl_str_partial_eq!(DiskName, str);
impl_str_partial_eq!(str, DiskName);
impl_str_partial_eq!(DiskName, &'a str);
impl_str_partial_eq!(&'a str, DiskName);
impl_str_partial_eq!(DiskName, String);
impl_str_partial_eq!(String, DiskName);

impl Eq for DiskName { }

impl Hash for DiskName {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state)
    }
}

impl Debug for DiskName {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_tuple("DiskName").field(&self.as_str()).finish()
    }
}

impl Display for DiskName {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str(self.as_str())
    }
}


impl<'de> Deserialize<'de> for DiskName {
    fn deserialize<D>(deserializer: D) -> Result<DiskName, D::Error>
    where
        D: Deserializer<'de>,
    {   
        deserializer.deserialize_str(ArcStrVisitor).map(|v| DiskName(v))
    }
}

impl Serialize for DiskName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}