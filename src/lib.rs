#![crate_type = "lib"]
#![allow(clippy::needless_lifetimes)]
#![feature(async_closure)]

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

mod api;
mod core_inner;

pub use self::api::grpc;
pub use self::core_inner::{
    backend, bob_client as client, configs, grinder, mapper, metrics, server, service,
};

mod prelude {
    pub(crate) use super::*;
    pub(crate) use grpc::{client::BobApiClient, Blob, BlobKey, BlobMeta, GetRequest, PutRequest};
    pub(crate) use std::{
        cell::{Cell, RefCell},
        collections::HashMap,
        convert::TryInto,
        fmt::{Debug, Display, Formatter, Result as FmtResult},
        fs::{create_dir_all, read_dir, read_to_string, remove_file, DirEntry, Metadata},
        io::{Error as IOError, ErrorKind, Result as IOResult},
        net::SocketAddr,
        path::PathBuf,
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll},
        time::{Duration, SystemTime},
    };
}
