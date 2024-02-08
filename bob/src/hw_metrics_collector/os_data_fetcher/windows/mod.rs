use std::{collections::{HashMap, HashSet}, path::Path};
use super::{DiskSpaceMetrics, OsDiskMetrics, OsDataMetrics};
use bob_common::core_types::{PhysicalDiskId, DiskPath};
use sysinfo::{DiskExt, System, SystemExt};

#[derive(Clone)]
pub(crate) struct OsDataFetcher {
    disks: HashSet<PhysicalDiskId>
}

impl OsDataFetcher {
    pub async fn new(bob_disks: &[DiskPath]) -> Self {
        let disks = find_physical_disks(bob_disks).await;
        println!("{:?}", disks);
        Self { disks }
    }

    pub async fn collect(&mut self) -> OsDataMetrics {
        OsDataMetrics::default()
    }

    pub async fn collect_space_metrics(&self) -> DiskSpaceMetrics {
        DiskSpaceMetrics::default()
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
                Some(d.mount_point().to_str().unwrap().into())
            } else {
                None
            }
        })
        .collect()
} 
