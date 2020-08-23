#![type_length_limit = "3167199"]
#![feature(proc_macro_hygiene, decl_macro, drain_filter)]
#![warn(clippy::pedantic)]
#![allow(clippy::used_underscore_binding)]
#![warn(missing_debug_implementations)]
#![warn(missing_docs)]

//! Library requires tokio runtime.

#[macro_use]
extern crate bitflags;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate cfg_if;
#[macro_use]
extern crate dipstick;
#[macro_use]
extern crate rocket;
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate derive_new;
#[macro_use]
extern crate lazy_static;

mod api;
mod core;

pub use self::api::grpc;
pub use self::core::data::DiskPath;
pub use self::core::{backend, bob_client as client, configs, grinder, mapper, metrics, server};

mod prelude {
    pub(crate) use super::*;
    pub(crate) use backend::data::{NodeDisk as DataNodeDisk, VDisk as DataVDisk};
    pub(crate) use grpc::{
        bob_api_client::BobApiClient, Blob, BlobKey, BlobMeta, GetRequest, PutRequest,
    };
    pub(crate) use std::{
        cell::{Ref, RefCell},
        collections::HashMap,
        convert::TryInto,
        fmt::{Debug, Display, Formatter, Result as FmtResult},
        fs::{create_dir_all, read_dir, read_to_string, remove_file, DirEntry, Metadata},
        io::{Cursor, Error as IOError, ErrorKind as IOErrorKind, Result as IOResult},
        net::SocketAddr,
        path::{Path, PathBuf},
        pin::Pin,
        sync::Arc,
        thread,
        time::{Duration, SystemTime},
    };
    pub(crate) use tokio::runtime::Runtime;
    pub(crate) use tokio::sync::RwLock;
}
