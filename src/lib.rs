#![crate_type = "lib"]
#![allow(clippy::needless_lifetimes)]

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

pub mod api;
pub mod core_inner;

mod prelude {
    // pub(crate) use futures::{future, task::Spawn, Future};
    // pub(crate) use futures_locks::RwLock;
    // pub(crate) use std::io::ErrorKind;
    // pub(crate) use std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration};
    // pub(crate) use stopwatch::Stopwatch;
    // pub(crate) use tokio::timer::Error as TimerError;
    // pub(crate) use tonic::{Code, Status};
    // pub(crate) use tonic::{Request, Response};
}
