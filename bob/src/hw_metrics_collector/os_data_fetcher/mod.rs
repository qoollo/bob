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
    pub(super) disk_space_metrics: HashMap<PhysicalDiskId, DiskSpaceMetrics>
}

#[cfg(target_family = "unix")]
mod unix;
#[cfg(target_family = "unix")]
pub(crate) use unix::OsDataFetcher;


#[cfg(not(any(target_family = "unix")))]
mod default {
    #[derive(Clone)]
    pub(super) struct OsDataFetcher {
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
            DiskSpaceMetrics { total_space: 0, used_space: 0, free_space: 0 }
        }
    }
}
