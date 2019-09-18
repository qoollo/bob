pub mod core;
pub mod mem_backend;
pub mod mem_tests;
pub mod stub_backend;

pub mod pearl;

mod policy;

use crate::core::data::VDiskId;
use std::io::ErrorKind;

#[derive(PartialEq, Debug)]
pub enum Error {
    Timeout,

    VDiskNoFound(VDiskId),
    StorageError(String),
    DuplicateKey,
    KeyNotFound,
    VDiskIsNotReady,

    Failed(String),
    Other,
}

impl Error {
    pub fn is_service(&self) -> bool {
        match &self {
            Error::Timeout | Error::Other | Error::Failed(_) => true,
            _ => false,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::VDiskNoFound(id) => write!(f, "vdisk: {:?} not found", id),
            Error::StorageError(description) => write!(f, "backend error: {}", description),
            err => write!(f, "{:?}", err),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        match error.kind() {
            ErrorKind::TimedOut => Error::Timeout,
            _ => Error::Failed(format!("Ping operation failed: {:?}", error)),
        }
    }
}
