use std::collections::{HashSet, HashMap};
use bob_common::core_types::{PhysicalDiskId, DiskPath};
use crate::hw_metrics_collector::CommandError;

use super::{DiskSpaceMetrics, OsDataMetrics};

pub(crate) mod cpu_iowait;

use cpu_iowait::CPUStatCollector;

#[derive(Clone)]
pub(crate) struct OsDataFetcher {
    disks: HashSet<PhysicalDiskId>,
    cpu_collector: CPUStatCollector
}

impl OsDataFetcher {
    pub async fn new(bob_disks: &[DiskPath]) -> Self {
        Self { disks: HashSet::default(), cpu_collector: CPUStatCollector::new() }
    }

    pub async fn collect(&mut self) -> OsDataMetrics {
        let mut result = OsDataMetrics::default();
        result.cpu_iowait = self.get_cpu_iowait();
        result 
    }

    pub async fn collect_space_metrics(&self) -> DiskSpaceMetrics {
        DiskSpaceMetrics { total_space: 0, used_space: 0, free_space: 0 }
    }

    fn get_cpu_iowait(&mut self) -> Option<f64> {
        match self.cpu_collector.iowait() {
            Ok(r) => Some(r),
            Err(e) => {
                match e {
                    CommandError::Unavailable => {},
                    CommandError::Primary(e) => warn!("failed to get iowait: {:?}", e),
                }
                None
            },
        }
    }
}

