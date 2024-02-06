use crate::{prelude::*, hw_metrics_collector::os_data_fetcher::OsDiskMetrics};
use std::collections::{HashSet, HashMap};

use bob_common::core_types::PhysicalDiskId;
use tokio::process::Command;

use super::{CommandError, parse_command_output, file_contents};

const DISK_STAT_FILE: &str = "/proc/diskstats";
const DIFFCONTAINER_THRESHOLD: Duration = Duration::from_millis(500);

#[derive(Clone)]
struct DiffContainer<T> {
    last: Option<T>,
}

impl<T> DiffContainer<T>
where T: std::ops::Sub<Output = T> +
         Copy,
{
    fn new() -> Self {
        Self {
            last: None,
        }
    }

    fn diff(&mut self, new: T) -> Option<T> {
        if let Some(last) = self.last {
            let diff = new - last;
            self.last = Some(new);
            Some(diff)
        } else {
            self.last = Some(new);
            None
        }
    }
}

#[derive(Clone, Copy)]
struct DiskStats {
    reads: u64,
    writes: u64,
    io_time: u64,
    extended: bool,
}

impl std::ops::Sub for DiskStats {
    type Output = DiskStats;
    fn sub(self, rhs: DiskStats) -> DiskStats {
        DiskStats {
            reads: self.reads.wrapping_sub(rhs.reads),
            writes: self.writes.wrapping_sub(rhs.writes),
            io_time: self.io_time.wrapping_sub(rhs.io_time),
            extended: self.extended && rhs.extended,
        }
    }
}

#[derive(Clone)]
struct DiskStatsContainer {
    path_str: PhysicalDiskId,
    stats: DiffContainer<DiskStats>,
}

#[derive(Clone)]
pub(super) struct DiskStatCollector {
    procfs_avl: bool,
    disk_metric_data: HashMap<String, DiskStatsContainer>,
    upd_timestamp: Instant,
}

impl DiskStatCollector {
    pub(super) async fn new(disks: &HashSet<PhysicalDiskId>) -> Self {
        // TODO Are disks always on unique devs?
        let mut disk_metric_data = HashMap::new();
        for path in disks {
            // TODO Do we need conversion to os_str?
            let path_str = std::path::Path::new(path.as_str()).as_os_str().to_str().unwrap();
            if let Ok(dev_name) = Self::dev_name(path_str).await {
                // let metric_prefix = format!("{}.{}", HW_DISKS_FOLDER, path_str);
                disk_metric_data.insert(dev_name, DiskStatsContainer { 
                    path_str: path_str.into(), stats: DiffContainer::new() 
                });
            } else {
                warn!("Device name of path {:?} is unknown", path);
            }
        }

        DiskStatCollector {
            procfs_avl: true,
            disk_metric_data,
            upd_timestamp: Instant::now(),
        }
    }

    fn parse_stat_line(parts: Vec<&str>) -> Result<DiskStats, CommandError> {
        let mut new_ds = DiskStats {
            reads: 0,
            writes: 0,
            io_time: 0,
            extended: parts.len() >= 12,
        };
        if let (Ok(r_ios), Ok(w_ios)) = (
            // successfull reads count is in 3rd column
            parts[3].parse::<u64>(),
            // successfull writes count is in 7th column
            parts[7].parse::<u64>(),
        ) {
            new_ds.reads = r_ios;
            new_ds.writes = w_ios;  
            if new_ds.extended {
                // time spend doing i/o operations is in 12th column
                if let Ok(io_time) = parts[12].parse::<u64>() {
                    new_ds.io_time = io_time;
                } else {
                    let msg = format!("Can't parse {} values to unsigned int", DISK_STAT_FILE);
                    return Err(CommandError::Primary(msg));
                }
            }
        } else {
            let msg = format!("Can't parse {} values to unsigned int", DISK_STAT_FILE);
            return Err(CommandError::Primary(msg));
        }
        Ok(new_ds)
    }

    pub(super) fn collect_metrics(&mut self) -> Result<HashMap<PhysicalDiskId, OsDiskMetrics>, CommandError> {
        let diskstats = self.diskstats()?;
        let now = Instant::now();
        let elapsed = now.duration_since(self.upd_timestamp);
        let mut result = HashMap::new();
        if elapsed > DIFFCONTAINER_THRESHOLD {
            self.upd_timestamp = now;
            let elapsed = elapsed.as_secs_f64();
            for i in diskstats {
                let lsp: Vec<&str> = i.split_whitespace().collect();
                if lsp.len() >= 8 {
                    let dev_name = lsp[2];
                    if let Some(ds) = self.disk_metric_data.get_mut(dev_name) {
                        let new_ds = Self::parse_stat_line(lsp)?;
                        
                        if let Some(diff) = ds.stats.diff(new_ds) {
                            let iops = (diff.reads + diff.writes) as f64 / elapsed;
                            let util = if diff.extended {
                                Some(diff.io_time as f64 / elapsed / 10.)
                            } else { 
                                None 
                            };
                            result.insert(ds.path_str.clone(), OsDiskMetrics { iops: Some(iops), util });
                        }
                    }
                } else {
                    self.procfs_avl = false;
                    let msg = format!("Not enough diskstat info in {} for metrics calculation", DISK_STAT_FILE);
                    return Err(CommandError::Primary(msg));
                }
            }
        }
        Ok(result)
    }

    async fn dev_name(disk_path: &str) -> Result<String, String> {
        let output = parse_command_output(Command::new("df").arg(disk_path)).await?;

        let mut lines = output.lines();
        lines.next(); // skip headers
        if let Some(line) = lines.next() {
            if let Some(raw_dev_name) = line.split_whitespace().next() {
                if let (Some(slash_ind), Some(non_digit_ind)) = (
                    // find where /dev/ ends
                    raw_dev_name.rfind('/'),
                    // find where partition digits start
                    raw_dev_name.rfind(|c: char| !c.is_digit(10)),
                ) {
                    return Ok(raw_dev_name[slash_ind + 1..=non_digit_ind].to_string());
                }
            }
        }

        Err("Error occured in dev_name getter".to_string())
    }

    fn diskstats(&mut self) -> Result<Vec<String>, CommandError> {
        if !self.procfs_avl {
            return Err(CommandError::Unavailable);
        }

        match file_contents(DISK_STAT_FILE) {
            Ok(lines) => Ok(lines),
            Err(e) => {
                self.procfs_avl = false;
                Err(CommandError::Primary(e))
            }
        }
    }
}
