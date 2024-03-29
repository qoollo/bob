pub mod core;
pub mod mem_backend;
pub mod pearl;
pub mod stub_backend;

#[cfg(test)]
pub mod mem_tests;

#[macro_use]
extern crate log;
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate metrics;
#[macro_use]
extern crate lazy_static;

pub(crate) mod prelude {
    pub use anyhow::{Context, Result as AnyResult};
    pub use bob_common::{
        configs::node::{BackendType, Node as NodeConfig, Pearl as PearlConfig},
        data::{BobData, BobKey, BobMeta},
        operation_options::{BobPutOptions, BobGetOptions, BobDeleteOptions},
        core_types::{DiskName, DiskPath, VDiskId},
        node::NodeName,
        error::Error,
        mapper::Virtual,
        metrics::BACKEND_STATE,
        stopwatch::Stopwatch,
    };
    pub use chrono::{DateTime, Datelike, Duration as ChronoDuration, NaiveDateTime, Utc};
    pub use futures::{stream::FuturesUnordered, StreamExt, TryFutureExt};
    pub use pearl::{
        filter::hierarchical::*, filter::traits::*, filter::Config as BloomConfig, Builder,
        ErrorKind as PearlErrorKind, IoDriver, Key as KeyTrait,
        RefKey as RefKeyTrait, Storage,
    };
    pub use std::{
        collections::HashMap,
        convert::TryInto,
        fmt::{Debug, Display, Formatter, Result as FmtResult},
        fs::Metadata,
        io::{Error as IOError, ErrorKind as IOErrorKind, Result as IOResult},
        path::{Path, PathBuf},
        sync::Arc,
        time::{Duration, Instant, SystemTime},
    };
    pub use tokio::{
        fs::{create_dir_all, read_dir, remove_dir_all, remove_file, DirEntry},
        sync::{RwLock, Semaphore},
    };
    pub use std::sync::RwLock as SyncRwLock;
}
