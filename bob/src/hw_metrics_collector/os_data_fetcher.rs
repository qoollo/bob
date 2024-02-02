use std::collections::{HashSet, HashMap};

use bob_common::core_types::{PhysicalDiskId, DiskPath};

use super::DiskSpaceMetrics;

#[derive(Clone)]
pub(super) struct OsDependentMetricsCollector {
    disks: HashSet<PhysicalDiskId>
}

pub(super) struct OsDiskMetrics {
    pub(super) iops: Option<f64>,
    pub(super) util: Option<f64>
}

#[derive(Default)]
pub(super) struct OsDependentMetrics {
    pub(super) descriptors_amount: Option<f64>,
    pub(super) cpu_iowait: Option<f64>,
    pub(super) disk_metrics: HashMap<PhysicalDiskId, OsDiskMetrics>,
    pub(super) disk_space_metrics: HashMap<PhysicalDiskId, DiskSpaceMetrics>
}

pub(super) struct OsDependentMetricsCollectorState;

impl OsDependentMetricsCollector {
    pub async fn new(bob_disks: &[DiskPath]) -> Self {
        Self { disks: HashSet::default() }
    }

    pub async fn collect(&self, state: &mut OsDependentMetricsCollectorState) -> OsDependentMetrics {
        OsDependentMetrics::default()
    }

    pub async fn collect_space_metrics(&self) -> DiskSpaceMetrics {
        DiskSpaceMetrics { total_space: 0, used_space: 0, free_space: 0 }
    }

    pub fn create_state(&self) -> OsDependentMetricsCollectorState {
        OsDependentMetricsCollectorState { }
    }
}

