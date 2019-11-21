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
pub(crate) use self::group::PearlGroup;
pub(crate) use self::metrics::init_pearl;
pub(crate) use super::prelude::*;

mod prelude {
    pub(crate) use super::*;

    pub(crate) use ::pearl::{Builder, ErrorKind, Key, Storage};
    pub(crate) use chrono::{DateTime, Datelike, Duration as ChronoDuration, NaiveDateTime, Utc};
    pub(crate) use configs::PearlConfig;
    pub(crate) use data::{BackendResult, Future03Result, PearlData, PearlKey, PearlStorage};
    pub(crate) use futures_locks::{RwLockReadGuard, RwLockWriteGuard};
    pub(crate) use group::PearlTimestampHolder;
    pub(crate) use holder::{PearlHolder, PearlSync};
    pub(crate) use metrics::{
        PEARL_GET_COUNTER, PEARL_GET_ERROR_COUNTER, PEARL_GET_TIMER, PEARL_PUT_COUNTER,
        PEARL_PUT_ERROR_COUNTER, PEARL_PUT_TIMER,
    };
    pub(crate) use settings::Settings;
    pub(crate) use stuff::{LockGuard, Stuff, SyncState};
    pub(crate) use tokio::timer::delay_for;
}
