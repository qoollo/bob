use crate::prelude::*;
use bob_common::metrics::{
    BOB_RAM, CPU_IOWAIT, CPU_LOAD, DESCRIPTORS_AMOUNT, FREE_RAM, FREE_SPACE, HW_DISKS_FOLDER,
    TOTAL_RAM, TOTAL_SPACE, USED_RAM, USED_SPACE,
};
use libc::statvfs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{self, Command};
use std::fs::File;
use std::io::{Read};
use sysinfo::{DiskExt, ProcessExt, RefreshKind, System, SystemExt};

const DESCRS_DIR: &str = "/proc/self/fd/";
const STAT_FILE: &str = "/proc/stat";

pub(crate) struct DiskSpaceMetrics {
    pub(crate) total_space: u64,
    pub(crate) used_space: u64,
    pub(crate) free_space: u64,
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
                        let p_md = path.metadata().expect("Can't get metadata from OS");
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

    pub(crate) fn update_space_metrics(&self) -> DiskSpaceMetrics {
        Self::update_space_metrics_from_disks(&self.disks)
    }

    async fn task(t: Duration, disks: HashMap<PathBuf, String>) {
        let mut interval = interval(t);
        let mut sys = System::new_all();
        let mut dcounter = DescrCounter::new();
        let mut cpu_s_c = CPUStatCollector::new();
        let mut disk_s_c = DiskStatCollector::new(&disks);
        let total_mem = kb_to_mb(sys.total_memory());
        gauge!(TOTAL_RAM, total_mem as f64);
        debug!("total mem in mb: {}", total_mem);
        let pid = std::process::id() as i32;

        loop {
            interval.tick().await;

            sys.refresh_specifics(
                RefreshKind::new()
                    .with_processes()
                    .with_disks()
                    .with_memory(),
            );

            match cpu_s_c.iowait() {
                Ok(iowait) => {
                    gauge!(CPU_IOWAIT, iowait);
                },
                Err(CommandError::Primary(e)) => {
                    warn!("Error while collecting cpu iowait: {}", e);
                },
                Err(CommandError::Unavailable) => (),
            }

            if let Some(proc) = sys.process(pid) {
                gauge!(CPU_LOAD, proc.cpu_usage() as f64);
                let bob_ram = kb_to_mb(proc.memory());
                gauge!(BOB_RAM, bob_ram as f64);
            } else {
                debug!("Can't get process stat descriptor");
            }

            let _ = Self::update_space_metrics_from_disks(&disks);
            let used_mem = kb_to_mb(sys.used_memory());
            debug!("used mem in mb: {}", used_mem);
            gauge!(USED_RAM, used_mem as f64);
            gauge!(FREE_RAM, (total_mem - used_mem) as f64);
            gauge!(DESCRIPTORS_AMOUNT, dcounter.descr_amount() as f64);

            if let Err(CommandError::Primary(e)) = disk_s_c.collect_and_send_metrics() {
                warn!("Error while collecting stats of disks: {}", e);
            }
        }
    }

    fn update_space_metrics_from_disks(disks: &HashMap<PathBuf, String>) -> DiskSpaceMetrics {
        let disks_metrics = Self::space(disks);
        gauge!(TOTAL_SPACE, bytes_to_mb(disks_metrics.total_space) as f64);
        gauge!(USED_SPACE, bytes_to_mb(disks_metrics.used_space) as f64);
        gauge!(FREE_SPACE, bytes_to_mb(disks_metrics.free_space) as f64);
        disks_metrics
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

    fn space(disks: &HashMap<PathBuf, String>) -> DiskSpaceMetrics {
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

        DiskSpaceMetrics {
            total_space: total,
            used_space: used,
            free_space: free,
        }
    }
}

enum CommandError {
    Unavailable,
    Primary(String),
}
struct CPUStatCollector {
    procfs_avl: bool,
}

impl CPUStatCollector {
    fn new() -> CPUStatCollector {
        CPUStatCollector {
            procfs_avl: true
        }
    }

    fn stat_cpu_line() -> Result<String, String> {
        if let Ok(mut stat_file) = File::open(STAT_FILE) {
            let mut contents = String::new();
            if stat_file.read_to_string(&mut contents).is_err() {
                return Err("Failed to read stat file".into());
            }
            for stat_line in contents.lines() {
                let mut parts = stat_line.split_whitespace();
                if let Some("cpu") = parts.next() {
                    return Ok(stat_line.into())
                }
            }
            Err("Can't find cpu stat".into())
        } else {
            Err("Can't open stat file".into())
        }
    }

    fn iowait(&mut self) -> Result<f64, CommandError> {
        if !self.procfs_avl {
            return Err(CommandError::Unavailable);
        }

        let mut err = None;
        match Self::stat_cpu_line() {
            Ok(line) => {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() > 5 {
                    let mut sum = 0.;
                    let mut f_iowait = 0.;
                    for i in 1..parts.len() {
                        match parts[i].parse::<f64>() {
                            Ok(val) => {
                                sum += val;
                                if i == 5 {
                                    f_iowait = val;
                                }
                            },
                            Err(_) => {
                                err = Some("Can't parse stat".to_string());
                                break;
                            }
                        }
                    }
                    if err.is_none() {
                        return Ok(f_iowait * 100. / sum);
                    }
                } else {
                    err = Some("Stat format changed".into());
                }
            },
            Err(e) => {
                err = Some(e);
            }
        }
        self.procfs_avl = false;
        Err(CommandError::Primary(err.unwrap()))
    }
}

struct DiskStatCollector {
    iostat_avl: bool,
    disk_metric_data: HashMap<String, String>,
}

impl DiskStatCollector {
    fn new(disks: &HashMap<PathBuf, String>) -> Self {
        let mut disk_metric_data = HashMap::new();
        for (path, disk_name) in disks {
            let path_str = path.as_os_str().to_str().unwrap();
            if let Ok(dev_name) = Self::dev_name(path_str) {
                let metric_prefix = format!("{}.{}", HW_DISKS_FOLDER, disk_name);
                disk_metric_data.insert(dev_name, metric_prefix);
            } else {
                warn!("Device name of disk {} is unknown", disk_name);
            }
        }

        DiskStatCollector {
            iostat_avl: true,
            disk_metric_data,
        }
    }

    fn collect_and_send_metrics(&mut self) -> Result<(), CommandError> {
        let iostat_lines = self.collect_iostat()?;
        for i in iostat_lines {
            let lsp: Vec<&str> = i.split_whitespace().collect();
            if lsp.len() == 21 {
                if let Some(metric_prefix) = self.disk_metric_data.get(lsp[0]) {
                    if let (Ok(rs), Ok(ws)) = (
                        // readops per s count is in 1st column
                        Self::parse_iostat_result(lsp[1]),
                        // writeops per s count is in 7th column
                        Self::parse_iostat_result(lsp[7]),
                    ) {
                        let iops = rs + ws;

                        let gauge_name = format!("{}_iops", metric_prefix);
                        gauge!(gauge_name, iops);
                    } else {
                        return Err(CommandError::Primary("Can't parse iostat output into float".to_string()));
                    }

                    if let (Ok(rwait), Ok(wwait)) = (
                        // readops wait time is in 5th column
                        Self::parse_iostat_result(lsp[5]),
                        // writeops wait time is in 11th column
                        Self::parse_iostat_result(lsp[11]),
                    ) {
                        let iowait = rwait + wwait;

                        let gauge_name = format!("{}_iowait", metric_prefix);
                        gauge!(gauge_name, iowait);
                    } else {
                        return Err(CommandError::Primary("Can't parse iostat output into float".to_string()));
                    }
                }
            } else if lsp.len() > 0 {
                return Err(CommandError::Primary("iostat output format changed, update this code".to_string()));
            }
        }
        Ok(())
    }

    fn dev_name(disk_path: &str) -> Result<String, String> {
        let output = parse_command_output(Command::new("df").arg(disk_path))?;

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

    fn parse_iostat_result(iostat_str: &str) -> Result<f64, std::num::ParseFloatError> {
        iostat_str.replace(',', ".").parse()
    }

    fn collect_iostat(&mut self) -> Result<Vec<String>, CommandError> {
        if !self.iostat_avl {
            return Err(CommandError::Unavailable);
        }

        match parse_command_output(Command::new("iostat").arg("-dx")) {
            Ok(output) => {
                let line_iter = output.lines();
                // skip headers
                let line_iter = line_iter.skip(3);
                Ok(line_iter.map(|line| line.to_string()).collect())
            }
            Err(e) => {
                self.iostat_avl = false;
                Err(CommandError::Primary(e))
            }
        }
    }
}

// this constant means, that `descriptors amount` value will be recalculated only on every
// `CACHED_TIMES`-th `descr_amount` function call (on other hand last calculated (i.e. cached) value
// will be returned)
const CACHED_TIMES: usize = 10;

struct DescrCounter {
    value: u64,
    cached_times: usize,
    lsof_enabled: bool,
}

impl DescrCounter {
    fn new() -> Self {
        DescrCounter {
            value: 0,
            cached_times: 0,
            lsof_enabled: true,
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
        let lsof_str = format!(
            "lsof -a -p {} -d ^mem -d ^cwd -d ^rtd -d ^txt -d ^DEL",
            process::id()
        );
        match pipers::Pipe::new(&lsof_str).then("wc -l").finally() {
            Ok(proc) => {
                match proc.wait_with_output() {
                    Ok(output) => {
                        if output.status.success() {
                            let count = String::from_utf8(output.stdout).unwrap();
                            match count[..count.len() - 1].parse::<u64>() {
                                Ok(count) => {
                                    return Some(count - 5); // exclude stdin, stdout, stderr, lsof pipe and wc pipe
                                }
                                Err(e) => {
                                    debug!("failed to parse lsof result: {}", e);
                                }
                            }
                        } else {
                            debug!(
                                "something went wrong (fs /proc will be used): {}",
                                String::from_utf8(output.stderr).unwrap()
                            );
                        }
                    }
                    Err(e) => {
                        debug!("lsof output wait error (fs /proc will be used): {}", e);
                    }
                }
            }
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

fn parse_command_output(command: &mut Command) -> Result<String, String> {
    let output = command.output();
    let program = command.get_program().to_str().unwrap();
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
