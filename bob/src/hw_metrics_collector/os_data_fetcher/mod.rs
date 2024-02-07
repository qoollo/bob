use std::collections::HashMap;
use bob_common::core_types::PhysicalDiskId;
use super::DiskSpaceMetrics;

pub(super) struct OsDiskMetrics {
    pub(super) iops: Option<f64>,
    pub(super) util: Option<f64>
}

#[derive(Default)]
pub(super) struct OsDataMetrics {
    pub(super) descriptors_amount: Option<f64>,
    pub(super) cpu_iowait: Option<f64>,
    pub(super) disk_metrics: HashMap<PhysicalDiskId, OsDiskMetrics>,
}

#[cfg(target_family = "unix")]
mod unix;
#[cfg(target_family = "unix")]
pub(crate) use unix::OsDataFetcher;

#[cfg(target_family = "windows")]
mod windows;
#[cfg(target_family = "windows")]
pub(crate) use windows::OsDataFetcher;

#[cfg(not(any(target_family = "unix", target_family = "windows")))]
mod default {
    use std::collections::{HashSet, HashMap};
    use super::{DiskSpaceMetrics, OsDiskMetrics, OsDataMetrics};
    use bob_common::core_types::{PhysicalDiskId, DiskPath};

    #[derive(Clone)]
    pub(crate) struct OsDataFetcher {
        disks: HashSet<PhysicalDiskId>
    }

    impl OsDataFetcher {
        pub async fn new(_bob_disks: &[DiskPath]) -> Self {
            Self { disks: HashSet::default() }
        }

        pub async fn collect(&mut self) -> OsDataMetrics {
            OsDataMetrics::default()
        }

        pub async fn collect_space_metrics(&self) -> DiskSpaceMetrics {
            DiskSpaceMetrics::default()
        }
    }
}
#[cfg(not(any(target_family = "unix", target_family = "windows")))]
pub(crate) use default::OsDataFetcher;
