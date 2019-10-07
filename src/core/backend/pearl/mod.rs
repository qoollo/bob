mod core;
mod data;
mod group;
mod holder;
mod metrics;
mod settings;
mod stuff;

#[cfg(test)]
mod tests;

pub(crate) use self::core::PearlBackend;
pub(crate) use super::prelude::*;

mod prelude {
    pub(crate) use super::data::{
        BackendResult, Future03Result, PearlData, PearlKey, PearlStorage,
    };
    pub(crate) use super::group::{PearlGroup, PearlTimestampHolder};
    pub(crate) use super::holder::{PearlHolder, PearlSync};
    pub(crate) use super::metrics::{
        PEARL_GET_COUNTER, PEARL_GET_ERROR_COUNTER, PEARL_GET_TIMER, PEARL_PUT_COUNTER,
        PEARL_PUT_ERROR_COUNTER, PEARL_PUT_TIMER,
    };
    pub(crate) use super::settings::Settings;
    pub(crate) use super::stuff::{LockGuard, Stuff, SyncState};
    pub(crate) use super::*;
    pub(crate) use crate::core::configs::PearlConfig;
    pub(crate) use crate::core::metrics::MetricsContainerBuilder;
    pub(crate) use ::pearl::{Builder, ErrorKind, Key, Storage};
    pub(crate) use chrono::{DateTime, NaiveDateTime, Utc};
    pub(crate) use dipstick::{Counter, Proxy, Timer};
    pub(crate) use std::fs::{create_dir_all, read_dir, remove_file, DirEntry, Metadata};
    pub(crate) use std::time::SystemTime;
    pub(crate) use std::{convert::TryInto, marker::PhantomData, path::PathBuf};
    pub(crate) use tokio::timer::delay_for;
}
