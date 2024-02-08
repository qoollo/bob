use std::collections::HashSet;
use super::{DiskSpaceMetrics, OsDataMetrics};
use bob_common::core_types::{PhysicalDiskId, DiskPath};
use sysinfo::{DiskExt, System, SystemExt};

#[derive(Clone)]
pub(crate) struct OsDataFetcher {
    disks: HashSet<PhysicalDiskId>
}

impl OsDataFetcher {
    pub async fn new(bob_disks: &[DiskPath]) -> Self {
        let disks = find_physical_disks(bob_disks).await;
        Self { disks }
    }

    pub async fn collect(&mut self) -> OsDataMetrics {
        OsDataMetrics::default()
    }

    pub async fn collect_space_metrics(&self) -> DiskSpaceMetrics {
        let mut total = 0;
        let mut used = 0;
        let mut free = 0;

        for disk in System::new_all().disks()
            .iter()
            .filter(|d| self.disks.contains(&get_id(d))) {

            let disk_total = disk.total_space();
            let disk_free = disk.available_space();
            let disk_used = disk_total - disk_free;
            
            total += disk_total;
            free += disk_free;
            used += disk_used;
        }

        DiskSpaceMetrics { 
            total_space: total,
            used_space: used,
            free_space: free
        }
    }
}

async fn find_physical_disks(bob_disks: &[DiskPath]) -> HashSet<PhysicalDiskId> {
    System::new_all()
        .disks()
        .iter()
        .filter_map(|d| {
            let path = std::fs::canonicalize(d.mount_point())
                .expect("canonicalize mount path");
            if bob_disks
                .iter()
                .any(move |dp| {
                    let dp = std::fs::canonicalize(dp.path())
                        .expect("canonicalize bob path");
                    dp.starts_with(&path)
                }) {
                Some(get_id(d))
            } else {
                None
            }
        })
        .collect()
}

fn get_id(d: &sysinfo::Disk) -> PhysicalDiskId {
    d.mount_point().to_str().unwrap().into()
} 
