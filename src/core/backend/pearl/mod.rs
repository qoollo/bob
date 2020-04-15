mod core;
mod data;
mod group;
mod holder;
mod metrics;
mod settings;
mod stuff;

#[cfg(test)]
mod tests;

pub(crate) use self::core::{BackendResult, Pearl, PearlStorage};
pub(crate) use self::group::Group;
pub(crate) use self::holder::Holder;
pub(crate) use self::metrics::init_pearl;
pub(crate) use super::prelude::*;

mod prelude {
    pub(crate) use super::*;

    pub(crate) use ::pearl::{Builder, ErrorKind, Key as KeyTrait, Storage};
    pub(crate) use chrono::{DateTime, Datelike, Duration as ChronoDuration, NaiveDateTime, Utc};
    pub(crate) use configs::Pearl as PearlConfig;
    pub(crate) use data::{Data, Key};
    pub(crate) use holder::Holder;
    pub(crate) use metrics::{
        PEARL_GET_COUNTER, PEARL_GET_ERROR_COUNTER, PEARL_GET_TIMER, PEARL_PUT_COUNTER,
        PEARL_PUT_ERROR_COUNTER, PEARL_PUT_TIMER,
    };
    pub(crate) use settings::Settings;
    pub(crate) use stuff::{Stuff, SyncState};
    pub(crate) use tokio::time::delay_for;
}
