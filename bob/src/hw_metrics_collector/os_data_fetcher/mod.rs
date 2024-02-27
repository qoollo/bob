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

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub(crate) use linux::OsDataFetcher;

#[cfg(not(target_os = "linux"))]
mod generic;
#[cfg(not(target_os = "linux"))]
pub(crate) use generic::OsDataFetcher;