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
        configs::{
            cluster::Cluster as ClusterConfig,
            node::{BackendType, Node as NodeConfig, Pearl as PearlConfig},
        },
        data::{BobData, BobKey, BobMeta, BobOptions, DiskPath, VDiskId},
        error::Error,
        mapper::Virtual,
        metrics::BACKEND_STATE,
    };
    pub use chrono::{DateTime, Datelike, Duration as ChronoDuration, NaiveDateTime, Utc};
    pub use futures::{stream::FuturesUnordered, StreamExt, TryFutureExt};
    pub use pearl::{
        filter::Config as BloomConfig, rio, Builder, Error as PearlError,
        ErrorKind as PearlErrorKind, Key as KeyTrait, Storage,
    };
    pub use std::{
        collections::{hash_map::Entry, HashMap},
        convert::TryInto,
        fmt::{Debug, Display, Formatter, Result as FmtResult},
        fs::Metadata,
        io::{Error as IOError, ErrorKind as IOErrorKind, Result as IOResult},
        path::{Path, PathBuf},
        sync::Arc,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };
    pub use stopwatch::Stopwatch;
    pub use tokio::{
        fs::{create_dir_all, read_dir, remove_dir_all, remove_file, DirEntry},
        sync::{RwLock, Semaphore},
    };
}
