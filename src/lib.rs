#![feature(proc_macro_hygiene, decl_macro, drain_filter)]
// #![deny(missing_debug_implementations)]
// #![deny(missing_docs)]
// #![warn(clippy::pedantic)]

//! Library requires tokio runtime

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

mod api;
mod core_inner;

pub use self::api::grpc;
pub use self::core_inner::{
    backend, bob_client as client, configs, grinder, mapper, metrics, server,
};

mod prelude {
    pub(crate) use super::*;
    pub(crate) use backend::data::{NodeDisk as DataNodeDisk, VDisk as DataVDisk};
    pub(crate) use grpc::{
        bob_api_client::BobApiClient, Blob, BlobKey, BlobMeta, GetRequest, PutRequest,
    };
    pub(crate) use std::{
        cell::{Cell, RefCell},
        collections::HashMap,
        convert::TryInto,
        fmt::{Debug, Display, Formatter, Result as FmtResult},
        fs::{create_dir_all, read_dir, read_to_string, remove_file, DirEntry, Metadata},
        io::{Cursor, Error as IOError, ErrorKind, Result as IOResult},
        net::SocketAddr,
        path::{Path, PathBuf},
        pin::Pin,
        sync::{Arc, Mutex},
        thread,
        time::{Duration, SystemTime},
    };
}
