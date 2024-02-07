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
