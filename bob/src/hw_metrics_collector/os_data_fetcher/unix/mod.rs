use std::collections::{HashSet, HashMap};
use bob_common::core_types::{PhysicalDiskId, DiskPath};
use crate::hw_metrics_collector::CommandError;
use self::descr_amount::DescrCounter;

use super::{DiskSpaceMetrics, OsDataMetrics};
use cpu_iowait::CPUStatCollector;

mod cpu_iowait;
mod descr_amount;

#[derive(Clone)]
pub(crate) struct OsDataFetcher {
    disks: HashSet<PhysicalDiskId>,
    cpu_collector: CPUStatCollector,
    descr_counter: DescrCounter
}

impl OsDataFetcher {
    pub async fn new(bob_disks: &[DiskPath]) -> Self {
        Self { 
            disks: HashSet::default(), 
            cpu_collector: CPUStatCollector::new(),
            descr_counter: DescrCounter::new()
        }
    }

    pub async fn collect(&mut self) -> OsDataMetrics {
        OsDataMetrics {
            cpu_iowait: self.get_cpu_iowait(),
            descriptors_amount: Some(self.descr_counter.descr_amount().await as f64),
            disk_metrics: HashMap::new(),
            disk_space_metrics: HashMap::new()
        }
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

