use crate::prelude::*;
use bob_common::metrics::{
    CPU_IOWAIT, CPU_LOAD, DESCRIPTORS_AMOUNT, FREE_RAM, FREE_SPACE, TOTAL_RAM, TOTAL_SPACE,
    USED_RAM, USED_SPACE,
};
use std::path::{Path, PathBuf};
use std::process;
use std::os::unix::fs::MetadataExt;
use sysinfo::{DiskExt, ProcessExt, System, SystemExt, RefreshKind};
use libc::statvfs;

const DESCRS_DIR: &str = "/proc/self/fd/";

struct DisksMetrics {
    total_space: u64,
    used_space: u64,
    free_space: u64,
}

pub(crate) struct HWMetricsCollector {
    disks: HashMap<PathBuf, String>,
    interval_time: Duration,
}

impl HWMetricsCollector {
    pub(crate) fn new(mapper: Arc<Virtual>, interval_time: Duration) -> Self {
        let disks = Self::collect_used_disks(mapper.local_disks());
        Self {
            disks,
            interval_time,
        }
    }

    fn collect_used_disks(disks: &[DiskPath]) -> HashMap<PathBuf, String> {
        System::new_all()
            .disks()
            .iter()
            .filter_map(|d| {
                let path = d.mount_point();
                disks
                    .iter()
                    .find(move |dp| {
                        let dp_md = Path::new(dp.path())
                    				.metadata()
                    				.expect("Can't get metadata from OS");
                        let p_md = path.metadata()
                                       .expect("Can't get metadata from OS");
                        p_md.dev() == dp_md.dev()               
                    })
                    .map(|config_disk| {
                        let diskpath = path.to_str().expect("Not UTF-8").to_owned();
                        (PathBuf::from(diskpath), config_disk.name().to_owned())
                    })
            })
            .collect()
    }

    pub(crate) fn spawn_task(&self) {
        tokio::spawn(Self::task(self.interval_time, self.disks.clone()));
    }

    async fn task(t: Duration, disks: HashMap<PathBuf, String>) {
        let mut interval = interval(t);
        let mut sys = System::new_all();
        let mut dcounter = DescrCounter::new();
        let mut cpu_s_c = CPUStatCollector::new();
        let total_mem = kb_to_mb(sys.total_memory());
        gauge!(TOTAL_RAM, total_mem as f64);
        debug!("total mem in mb: {}", total_mem);
        let pid = std::process::id() as i32;

        loop {
            interval.tick().await;

            sys.refresh_specifics(RefreshKind::new().with_processes()
                                                    .with_disks()
                                                    .with_memory());
            gauge!(CPU_IOWAIT, cpu_s_c.iowait());
          
            if let Some(proc) = sys.process(pid) {
                gauge!(CPU_LOAD, proc.cpu_usage() as f64);
                let bob_ram = kb_to_mb(proc.memory());
                gauge!(BOB_RAM, bob_ram as f64);
            } else {
                debug!("Can't get process stat descriptor");
            }

            let disks_metrics = Self::space(&disks);
            gauge!(TOTAL_SPACE, bytes_to_mb(disks_metrics.total_space) as f64);
            gauge!(USED_SPACE, bytes_to_mb(disks_metrics.used_space) as f64);
            gauge!(FREE_SPACE, bytes_to_mb(disks_metrics.free_space) as f64);
            let used_mem = kb_to_mb(sys.used_memory());
            debug!("used mem in mb: {}", used_mem);
            gauge!(USED_RAM, used_mem as f64);
            gauge!(FREE_RAM, (total_mem - used_mem) as f64);
            gauge!(DESCRIPTORS_AMOUNT, dcounter.descr_amount() as f64);
        }
    }

    fn to_cpath(path: &Path) -> Vec<u8> {
        use std::{ffi::OsStr, os::unix::ffi::OsStrExt};
    
        let path_os: &OsStr = path.as_ref();
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

    // FIXME: maybe it's better to cache needed disks, but I am not sure, that they would be
    // refreshed, if I clone them
    // NOTE: HashMap contains only needed mount points of used disks, so it won't be really big,
    // but maybe it's more efficient to store disks (instead of mount_points) and update them one by one
    fn space(disks: &HashMap<PathBuf, String>) -> DisksMetrics {
        let mut total = 0;
        let mut used = 0;
        let mut free = 0;
        
        for mount_point in disks.keys() {
            let cm_p = Self::to_cpath(mount_point.as_path());
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

        DisksMetrics {
            total_space: total,
            used_space: used,
            free_space: free
        }
    }
}

struct CPUStatCollector {
    iostat_avl: bool,
}

impl CPUStatCollector {
    fn new() -> CPUStatCollector {
        CPUStatCollector { iostat_avl: true }
    }

    fn parse_iowait_result(iowait_str: &str) -> Result<f64, std::num::ParseFloatError> {
        iowait_str.replace(',', ".").parse()
    }

    fn iowait(&mut self) -> f64 {
        if !self.iostat_avl {
            return -1.;
        }

        let ios = std::process::Command::new("iostat").arg("-c").output();
        match ios {
            Ok(output) => {
                if let (true, Ok(out)) = (output.status.success(), String::from_utf8(output.stdout))
                {
                    // find avg-cpu headers line
                    let mut lines = out
                        .lines()
                        .skip_while(|line| line.find("avg-cpu:").is_none());
                    // find iowait column on the next line
                    if let Some(line) = lines.next() {
                        let mut values = line.split_whitespace();
                        // iowait is in 3th column
                        if let Some(iowait_str) = values.nth(3) {
                            if let Ok(iowait) = Self::parse_iowait_result(iowait_str) {
                                return iowait;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                debug!("Failed to execute iostat: {}", e);
                self.iostat_avl = false;
            }
        };
        -1.
    }
}

// this constant means, that `descriptors amount` value will be recalculated only on every
// `CACHED_TIMES`-th `descr_amount` function call (on other hand last calculated (i.e. cached) value
// will be returned)
const CACHED_TIMES: usize = 10;

struct DescrCounter {
    value: u64,
    cached_times: usize,
    lsof_enabled: bool
}

impl DescrCounter {
    fn new() -> Self {
        DescrCounter {
            value: 0,
            cached_times: 0,
            lsof_enabled: true
        }
    }

    fn descr_amount(&mut self) -> u64 {
        if self.cached_times == 0 {
            self.cached_times = CACHED_TIMES;
            self.value = self.count_descriptors();
        } else {
            self.cached_times -= 1;
        }
        self.value
    }

    fn count_descriptors_by_lsof(&mut self) -> Option<u64> {
        if !self.lsof_enabled {
            return None;
        }
        let lsof_str = format!("lsof -a -p {} -d ^mem -d ^cwd -d ^rtd -d ^txt -d ^DEL", process::id());
        match pipers::Pipe::new(&lsof_str)
                        .then("wc -l")
                        .finally() {
            Ok(proc) => {
                match proc.wait_with_output() {
                    Ok(output) => {
                        if output.status.success() {
                            let count = String::from_utf8(output.stdout).unwrap();
                            match count[..count.len() - 1].parse::<u64>() {
                                Ok(count) => {
                                    return Some(count - 5); // exclude stdin, stdout, stderr, lsof pipe and wc pipe
                                },
                                Err(e) => {
                                    debug!("failed to parse lsof result: {}", e);
                                }
                            }
                        } else {
                            debug!("something went wrong (fs /proc will be used): {}",
                                String::from_utf8(output.stderr).unwrap());
                        }
                    },
                    Err(e) => {
                        debug!("lsof output wait error (fs /proc will be used): {}", e);
                    }
                }
            },
            Err(e) => {
                debug!("can't use lsof (fs /proc will be used): {}", e);
            }
        }
        self.lsof_enabled = false;
        return None;
    }
    
    fn count_descriptors(&mut self) -> u64 {
        // FIXME: didn't find better way, but iterator's `count` method has O(n) complexity
        // isolated tests (notice that in this case directory may be cached, so it works more
        // quickly):
        // | fds amount | running (secs) |
        // | 1.000.000  |      0.6       |
        // |  500.000   |      0.29      |
        // |  250.000   |      0.15      |
        //
        //     for bob (tested on
        //                 Laptop: HP Pavilion Laptop 15-ck0xx,
        //                 OS: 5.12.16-1-MANJARO)
        //
        //  without payload:
        //  |  10.000   |      0.006     |
        //  with payload
        //  |  10.000   |      0.018     |
        if let Some(descr) = self.count_descriptors_by_lsof() {
            return descr;
        }

        let d = std::fs::read_dir(DESCRS_DIR);
        match d {
            Ok(d) => {
                d.count() as u64 - 4 // exclude stdin, stdout, stderr and `read_dir` instance
            }
            Err(e) => {
                debug!("failed to count descriptors: {}", e);
                0 // proc is unsupported
            }
        }
    }
}

fn bytes_to_mb(bytes: u64) -> u64 {
    bytes / 1024 / 1024
}

fn kb_to_mb(kbs: u64) -> u64 {
    kbs / 1024
}
