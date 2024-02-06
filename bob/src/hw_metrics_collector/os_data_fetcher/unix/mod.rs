use std::{collections::{HashSet, HashMap}, path::Path, os::unix::prelude::MetadataExt, fs::read_to_string};
use bob_common::core_types::{PhysicalDiskId, DiskPath};
use libc::statvfs;
use sysinfo::{System, SystemExt, DiskExt};
use tokio::process::Command;
use self::descr_amount::DescrCounter;

use super::{DiskSpaceMetrics, OsDataMetrics, OsDiskMetrics};
use cpu_iowait::CPUStatCollector;
use disk_metrics::DiskStatCollector;

mod cpu_iowait;
mod descr_amount;
mod disk_metrics;

enum CommandError {
    Unavailable,
    Primary(String),
}

#[derive(Clone)]
pub(crate) struct OsDataFetcher {
    disks: HashSet<PhysicalDiskId>,
    cpu_collector: CPUStatCollector,
    descr_counter: DescrCounter,
    disk_metrics_collector: DiskStatCollector
}

impl OsDataFetcher {
    pub async fn new(bob_disks: &[DiskPath]) -> Self {
        let disks = Self::find_physical_disks(bob_disks).await;
        let disk_metrics_collector = DiskStatCollector::new(&disks).await;
        Self { 
            disks,
            cpu_collector: CPUStatCollector::new(),
            descr_counter: DescrCounter::new(),
            disk_metrics_collector
        }
    }

    pub async fn collect(&mut self) -> OsDataMetrics {
        OsDataMetrics {
            cpu_iowait: self.get_cpu_iowait(),
            descriptors_amount: Some(self.descr_counter.descr_amount().await as f64),
            disk_metrics: self.get_disk_metrics()
        }
    }

    pub async fn collect_space_metrics(&self) -> DiskSpaceMetrics {
        let mut total = 0;
        let mut used = 0;
        let mut free = 0;

        for id in &self.disks {
            let cm_p = Self::to_cpath(id);
            let stat = Self::statvfs_wrap(&cm_p);
            if let Some(stat) = stat {
                let bsize = stat.f_bsize as u64;
                let blocks = stat.f_blocks as u64;
                let bavail = stat.f_bavail as u64;
                let bfree = stat.f_bfree as u64;
                total += bsize * blocks;
                free += bsize * bavail;
                used += (blocks - bfree) * bsize;
            }
        }

        DiskSpaceMetrics {
            total_space: total,
            used_space: used,
            free_space: free,
        }
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

    fn get_disk_metrics(&mut self) -> HashMap<PhysicalDiskId, OsDiskMetrics> {
        match self.disk_metrics_collector.collect_metrics() {
            Ok(r) => r,
            Err(e) => {
                match e {
                    CommandError::Unavailable => {},
                    CommandError::Primary(e) => warn!("failed to get disk metrics: {:?}", e),
                }
                HashMap::default()
            },
        }
    }

    async fn find_physical_disks(bob_disks: &[DiskPath]) -> HashSet<PhysicalDiskId> {
        System::new_all()
            .disks()
            .iter()
            .filter_map(|d| {
                let path = d.mount_point();
                if bob_disks
                    .iter()
                    .any(move |dp| {
                        let dp_md = Path::new(dp.path())
                            .metadata()
                            .expect("Can't get metadata from OS");
                        let p_md = path.metadata().expect("Can't get metadata from OS");
                        p_md.dev() == dp_md.dev()
                    }) {
                    Some(path.to_str().expect("Not UTF-8").into())
                } else {
                    None
                }
            })
            .collect()
    } 

    fn to_cpath(id: &PhysicalDiskId) -> Vec<u8> {
        use std::{ffi::OsStr, os::unix::ffi::OsStrExt};

        let path_os: &OsStr = Path::new(id.as_str()).as_ref();
        let mut cpath = path_os.as_bytes().to_vec();
        cpath.push(0);
        cpath
    }

    fn statvfs_wrap(mount_point: &Vec<u8>) -> Option<statvfs> {
        unsafe {
            let mut stat: statvfs = std::mem::zeroed();
            if statvfs(mount_point.as_ptr() as *const _, &mut stat) == 0 {
                Some(stat)
            } else {
                None
            }
        }
    }
}

async fn parse_command_output(command: &mut Command) -> Result<String, String> {
    let output = command.output().await;
    let program = command.as_std().get_program().to_str().unwrap();
    match output {
        Ok(output) => match (output.status.success(), String::from_utf8(output.stdout)) {
            (true, Ok(out)) => Ok(out),
            (false, _) => {
                if let Ok(e) = String::from_utf8(output.stderr) {
                    let error = format!("Command {} finished with error: {}", program, e);
                    Err(error)
                } else {
                    Err("Command finished with error".to_string())
                }
            }
            (_, Err(e)) => {
                let error = format!("Can not convert output of {} into string: {}", program, e);
                Err(error)
            }
        },
        Err(e) => {
            let error = format!("Failed to execute command {}: {}", program, e);
            Err(error)
        }
    }
}

fn file_contents(path: &str) -> Result<Vec<String>, String> {
    if let Ok(contents) = read_to_string(path) {
        Ok(contents.lines().map(|l| l.to_string()).collect())
    } else {
        Err(format!("Can't read file {}", path))
    }
}

