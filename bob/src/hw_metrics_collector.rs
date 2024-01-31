use crate::prelude::*;
use bob_common::core_types::DiskName;
use bob_common::metrics::{
    BOB_RAM, BOB_VIRTUAL_RAM, BOB_CPU_LOAD, DESCRIPTORS_AMOUNT, CPU_IOWAIT, 
    TOTAL_RAM, AVAILABLE_RAM, USED_RAM, USED_SWAP,
    TOTAL_SPACE, FREE_SPACE, USED_SPACE, HW_DISKS_FOLDER
};
use libc::statvfs;
use std::ops::{AddAssign, Add};
use std::iter::Sum;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use tokio::process::Command;
use std::fs::read_to_string;
use sysinfo::{DiskExt, ProcessExt, ProcessRefreshKind, RefreshKind, System, SystemExt};

const DESCRS_DIR: &str = "/proc/self/fd/";
const CPU_STAT_FILE: &str = "/proc/stat";
const DISK_STAT_FILE: &str = "/proc/diskstats";

#[derive(Debug, Clone)]
pub(crate) struct DiskSpaceMetrics {
    pub(crate) total: SpaceMetrics,
    pub(crate) per_disk: HashMap<PathBuf, SpaceMetrics>
}

#[derive(Debug, Clone)]
pub(crate) struct SpaceMetrics {
    pub(crate) total_space: u64,
    pub(crate) used_space: u64,
    pub(crate) free_space: u64,
}

pub(crate) struct HWMetricsCollector {
    disks: HashMap<PathBuf, DiskName>,
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

    fn collect_used_disks(disks: &[DiskPath]) -> HashMap<PathBuf, DiskName> {
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
                        (PathBuf::from(diskpath), config_disk.name().clone())
                    })
            })
            .collect()
    }

    pub(crate) fn spawn_task(&self) {
        tokio::spawn(Self::task(self.interval_time, self.disks.clone()));
    }

    /// Returns the updated space metrics of this [`HWMetricsCollector`].
    pub(crate) fn update_space_metrics(&self) -> DiskSpaceMetrics {
        Self::update_space_metrics_from_disks(&self.disks)
    }

    async fn task(t: Duration, disks: HashMap<PathBuf, DiskName>) {
        let mut interval = interval(t);
        let mut sys = System::new_all();
        let mut dcounter = DescrCounter::new();
        let mut cpu_s_c = CPUStatCollector::new();
        let mut disk_s_c = DiskStatCollector::new(&disks).await;
        let total_mem = sys.total_memory();
        gauge!(TOTAL_RAM, total_mem as f64);
        debug!("total mem in bytes: {}", total_mem);
        let pid = sysinfo::get_current_pid().expect("Cannot determine current process PID");

        loop {
            interval.tick().await;

            sys.refresh_specifics(
                RefreshKind::new()
                    .with_processes(ProcessRefreshKind::everything())
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
                gauge!(BOB_CPU_LOAD, proc.cpu_usage() as f64);
                let bob_ram = proc.memory();
                gauge!(BOB_RAM, bob_ram as f64);
                let bob_virtual_ram = proc.virtual_memory();
                gauge!(BOB_VIRTUAL_RAM, bob_virtual_ram as f64);
            } else {
                debug!("Can't get process stat descriptor");
            }

            let _ = Self::update_space_metrics_from_disks(&disks);
            let available_mem = sys.available_memory();
            let used_mem = total_mem - available_mem;
            let used_swap = sys.used_swap();
            debug!("used mem in bytes: {} | available mem in bytes: {} | used swap: {}", used_mem, available_mem, used_swap);
            gauge!(USED_RAM, used_mem as f64);
            gauge!(AVAILABLE_RAM, available_mem as f64);
            gauge!(USED_SWAP, used_swap as f64);
            let descr_amount = dcounter.descr_amount().await as f64;
            gauge!(DESCRIPTORS_AMOUNT, descr_amount);

            if let Err(CommandError::Primary(e)) = disk_s_c.collect_and_send_metrics() {
                warn!("Error while collecting stats of disks: {}", e);
            }
        }
    }

    fn update_space_metrics_from_disks(
        disks: &HashMap<PathBuf, DiskName>,
    ) -> DiskSpaceMetrics {
        let disks_metrics = Self::space(disks);
        let SpaceMetrics { total_space, used_space, free_space } = disks_metrics.values().sum();
        gauge!(TOTAL_SPACE, bytes_to_mb(total_space) as f64);
        gauge!(USED_SPACE, bytes_to_mb(used_space) as f64);
        gauge!(FREE_SPACE, bytes_to_mb(free_space) as f64);
        DiskSpaceMetrics {
                total: SpaceMetrics {
                    total_space,
                    used_space,
                    free_space,
                },
                per_disk: disks_metrics,
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

    /// Maps mount point to the corresponding [`SpaceMetrics`].
    ///
    /// Key -- mount points of used disks
    /// Value -- Up-to-date [`SpaceMetrics`]
    fn space(disks: &HashMap<PathBuf, DiskName>) -> HashMap<PathBuf, SpaceMetrics> {
        let mut res = HashMap::new();
        let mut fs_ids = HashSet::new();
        for (mount_point, _) in disks {
            let cm_p = Self::to_cpath(mount_point.as_path());
            let stat = Self::statvfs_wrap(&cm_p);
            if let Some(stat) = stat {
                let bsize: u64 = stat.f_bsize;
                let blocks: u64 = stat.f_blocks;
                let bavail: u64 = stat.f_bavail;
                let bfree: u64 = stat.f_bfree;
                if fs_ids.insert(stat.f_fsid) { 
                    res.insert(
                        mount_point.clone(),
                        SpaceMetrics {
                            total_space: bsize * blocks,
                            used_space: bsize * bavail,
                            free_space: (blocks - bfree) * bsize,
                        },
                    );
                }
            }
        }

        res
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
        let lines = file_contents(CPU_STAT_FILE)?;
        for stat_line in lines {
            if stat_line.starts_with("cpu") {
                return Ok(stat_line);
            }
        }
        Err(format!("Can't find cpu stat line in {}", CPU_STAT_FILE))
    }

    fn iowait(&mut self) -> Result<f64, CommandError> {
        if !self.procfs_avl {
            return Err(CommandError::Unavailable);
        }

        const CPU_IOWAIT_COLUMN: usize = 5;
        let mut err = None;
        match Self::stat_cpu_line() {
            Ok(line) => {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() > CPU_IOWAIT_COLUMN {
                    let mut sum = 0.;
                    let mut f_iowait = 0.;
                    for i in 1..parts.len() {
                        match parts[i].parse::<f64>() {
                            Ok(val) => {
                                sum += val;
                                if i == CPU_IOWAIT_COLUMN {
                                    f_iowait = val;
                                }
                            },
                            Err(_) => {
                                let msg = format!("Can't parse {}", CPU_STAT_FILE);
                                err = Some(msg);
                                break;
                            }
                        }
                    }
                    if err.is_none() {
                        return Ok(f_iowait * 100. / sum);
                    }
                } else {
                    let msg = format!("CPU stat format in {} changed", CPU_STAT_FILE);
                    err = Some(msg);
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

const DIFFCONTAINER_THRESHOLD: Duration = Duration::from_millis(500);

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

struct DiskStatsContainer {
    prefix: String,
    stats: DiffContainer<DiskStats>,
}

impl  DiskStatsContainer {
    fn new(prefix: String) -> Self {
        Self {
            prefix,
            stats: DiffContainer::new(),
        }
    }
}
struct DiskStatCollector {
    procfs_avl: bool,
    disk_metric_data: HashMap<String, DiskStatsContainer>,
    upd_timestamp: Instant,
}

impl DiskStatCollector {
    async fn new(disks: &HashMap<PathBuf, DiskName>) -> Self {
        let mut disk_metric_data = HashMap::new();
        for (path, disk_name) in disks {
            let path_str = path.as_os_str().to_str().unwrap();
            if let Ok(dev_name) = Self::dev_name(path_str).await {
                let metric_prefix = format!("{}.{}", HW_DISKS_FOLDER, disk_name);
                disk_metric_data.insert(dev_name, DiskStatsContainer::new(metric_prefix));
            } else {
                warn!("Device name of disk {} is unknown", disk_name);
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

    fn collect_and_send_metrics(&mut self) -> Result<(), CommandError> {
        let diskstats = self.diskstats()?;
        let now = Instant::now();
        let elapsed = now.duration_since(self.upd_timestamp);
        if elapsed > DIFFCONTAINER_THRESHOLD {
            self.upd_timestamp = now;
            let elapsed = elapsed.as_secs_f64();
            for i in diskstats {
                let lsp: Vec<&str> = i.split_whitespace().collect();
                if lsp.len() >= 8 {
                    // compare device name from 2nd column with disk device names
                    if let Some(ds) = self.disk_metric_data.get_mut(lsp[2]) {
                        let new_ds = Self::parse_stat_line(lsp)?;
                        
                        if let Some(diff) = ds.stats.diff(new_ds) {
                            let iops = (diff.reads + diff.writes) as f64 / elapsed;
                            let gauge_name = format!("{}_iops", ds.prefix);
                            gauge!(gauge_name, iops);

                            if diff.extended {
                                // disk util in %
                                let util = diff.io_time as f64 / elapsed / 10.;
                                let gauge_name = format!("{}_util", ds.prefix);
                                gauge!(gauge_name, util);
                            }
                        }
                    }
                } else {
                    self.procfs_avl = false;
                    let msg = format!("Not enough diskstat info in {} for metrics calculation", DISK_STAT_FILE);
                    return Err(CommandError::Primary(msg));
                }
            }
        }
        Ok(())
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

fn file_contents(path: &str) -> Result<Vec<String>, String> {
    if let Ok(contents) = read_to_string(path) {
        Ok(contents.lines().map(|l| l.to_string()).collect())
    } else {
        Err(format!("Can't read file {}", path))
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

    async fn descr_amount(&mut self) -> u64 {
        if self.cached_times == 0 {
            self.cached_times = CACHED_TIMES;
            self.value = self.count_descriptors().await;
        } else {
            self.cached_times -= 1;
        }
        self.value
    }

    async fn count_descriptors_by_lsof(&mut self) -> Option<u64> {
        if !self.lsof_enabled {
            return None;
        }

        let pid_arg = std::process::id().to_string();
        let cmd_lsof = Command::new("lsof")
            .args(["-a", "-p", &pid_arg, "-d", "^mem", "-d", "^cwd", "-d", "^rtd", "-d", "^txt", "-d", "^DEL"])
            .stdout(std::process::Stdio::piped())
            .spawn();
        match cmd_lsof {
            Ok(mut cmd_lsof) => {
                match cmd_lsof.stdout.take() {
                    Some(stdout) => {
                        match TryInto::<std::process::Stdio>::try_into(stdout) {
                            Ok(stdio) => {
                                match parse_command_output(Command::new("wc").arg("-l").stdin(stdio)).await {
                                    Ok(output) => {
                                        match output.trim().parse::<u64>() {
                                            Ok(count) => {
                                                if let Err(e) = cmd_lsof.wait().await {
                                                    debug!("lsof exited with error {}", e);
                                                }
                                                return Some(count - 5); // exclude stdin, stdout, stderr, lsof pipe and wc pipe
                                            }
                                            Err(e) => {
                                                debug!("failed to parse lsof result: {}", e);
                                            }
                                        }
                                    },
                                    Err(e) => {
                                        debug!("can't use lsof, wc error (fs /proc will be used): {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                debug!("failed to parse stdout of lsof to stdio: {}", e);
                            }
                        }
                    },
                    None => {
                        debug!("lsof has no stdout (fs /proc will be used)");
                    }
                }
                if let Err(e) = cmd_lsof.wait().await {
                    debug!("lsof exited with error {}", e);
                }
            },
            Err(e) => {
                debug!("can't use lsof (fs /proc will be used): {}", e);
            }
        }
        self.lsof_enabled = false;
        None
    }

    async fn count_descriptors(&mut self) -> u64 {
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
        if let Some(descr) = self.count_descriptors_by_lsof().await {
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

// Std Traits Impls

impl<'a> AddAssign<&'a Self> for SpaceMetrics {
    fn add_assign(&mut self, rhs: &Self) {
        self.used_space += rhs.used_space;
        self.free_space += rhs.free_space;
        self.total_space += rhs.total_space;
    }
}
impl<'a> Add<&'a Self> for SpaceMetrics {
    type Output = Self;

    fn add(mut self, rhs: &Self) -> Self::Output {
        self += rhs;
        self
    }
}

impl<'a> Sum<&'a Self> for SpaceMetrics {
    /// Summarize [`SpaceMetrics`] over an iterator.
    /// NOTE: the `disk_name` field will be chosen from the first appeared disk in iterator if there
    /// is any. Otherwise 'None' will be passed
    fn sum<I: Iterator<Item = &'a Self>>(mut iter: I) -> Self {
        let init = if let Some(metrics) = iter.next() {
            metrics.clone()
        } else {
            return Self {
                total_space: 0,
                used_space: 0,
                free_space: 0,
            }
        };
        iter.fold(
            init,
            |a, b| a + b,
        )
    }
}
