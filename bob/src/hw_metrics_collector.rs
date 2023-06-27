use crate::prelude::*;
use bob_common::metrics::{
    CPU_LOAD, DESCRIPTORS_AMOUNT, FREE_RAM, FREE_SPACE, TOTAL_RAM, TOTAL_SPACE, USED_RAM,
    USED_SPACE, BOB_RAM, USED_SWAP, BOB_VIRTUAL_RAM
};
use std::path::{Path, PathBuf};
use std::os::unix::fs::MetadataExt;
use sysinfo::{DiskExt, ProcessExt, System, SystemExt, RefreshKind};

const DESCRS_DIR: &str = "/proc/self/fd/";

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
        System::new_with_specifics(RefreshKind::new().with_disks_list().with_disks())
            .disks()
            .iter()
            .filter_map(|d| {
                let path = d.mount_point();
                let path_metadata = match path.metadata() {
                    Ok(meta) => meta,
                    Err(err) => {
                        warn!("Can't get metadata from OS for disk '{}' returned by 'sysinfo': {:?}", path.display(), err);
                        return None;
                    }
                };
                disks
                    .iter()
                    .find(move |dp| {
                        match Path::new(dp.path()).metadata() {
                            Ok(dp_metadata) => {
                                path_metadata.dev() == dp_metadata.dev()
                            },
                            Err(err) => {
                                warn!("Can't get metadata from OS for disk '{}' specified in cluster config: {:?}", dp.path(), err);
                                false
                            }
                        }
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
        let total_mem = kb_to_b(sys.total_memory());
        gauge!(TOTAL_RAM, total_mem as f64);
        debug!("total mem in bytes: {}", total_mem);
        let pid = std::process::id() as i32;

        loop {
            interval.tick().await;
            sys.refresh_specifics(RefreshKind::new()
                    .with_processes()
                    .with_disks()
                    .with_memory());
            let proc = sys.process(pid).expect("Can't get process stat descriptor");

            let (total_space, free_space) = Self::space(&sys, &disks);
            gauge!(TOTAL_SPACE, total_space as f64);
            gauge!(USED_SPACE, (total_space - free_space) as f64);
            gauge!(FREE_SPACE, free_space as f64);
            let used_mem = kb_to_b(sys.used_memory());
            debug!("used mem in bytes: {}", used_mem);
            gauge!(USED_RAM, used_mem as f64);
            gauge!(FREE_RAM, (total_mem - used_mem) as f64);
            let used_swap = kb_to_b(sys.used_swap());
            gauge!(USED_SWAP, used_swap as f64);
            let bob_ram = kb_to_b(proc.memory());
            gauge!(BOB_RAM, bob_ram as f64);
            let bob_virtual_ram = kb_to_b(proc.virtual_memory());
            gauge!(BOB_VIRTUAL_RAM, bob_virtual_ram as f64);
            gauge!(DESCRIPTORS_AMOUNT, dcounter.descr_amount() as f64);
            gauge!(CPU_LOAD, proc.cpu_usage() as f64);
        }
    }

    // FIXME: maybe it's better to cache needed disks, but I am not sure, that they would be
    // refreshed, if I clone them
    // NOTE: HashMap contains only needed mount points of used disks, so it won't be really big,
    // but maybe it's more efficient to store disks (instead of mount_points) and update them one by one
    fn space(sys: &System, disks: &HashMap<PathBuf, String>) -> (u64, u64) {
        sys.disks()
            .iter()
            .filter_map(|disk| {
                disks
                    .get(disk.mount_point())
                    .map(|diskname| (disk, diskname))
            })
            .fold((0, 0), |(total, free), (disk, diskname)| {
                let disk_total = bytes_to_mb(disk.total_space());
                let disk_free = bytes_to_mb(disk.available_space());
                trace!(
                    "{} (with path {}): total = {}, free = {};",
                    diskname,
                    disk.mount_point()
                        .to_str()
                        .expect("Can't parse mount point"),
                    disk_total,
                    disk_free
                );
                (total + disk_total, free + disk_free)
            })
    }
}

// this constant means, that `descriptors amount` value will be recalculated only on every
// `CACHED_TIMES`-th `descr_amount` function call (on other hand last calculated (i.e. cached) value
// will be returned)
const CACHED_TIMES: usize = 10;

struct DescrCounter {
    value: u64,
    cached_times: usize,
}

impl DescrCounter {
    fn new() -> Self {
        DescrCounter {
            value: 0,
            cached_times: 0,
        }
    }

    fn descr_amount(&mut self) -> u64 {
        if self.cached_times == 0 {
            self.cached_times = CACHED_TIMES;
            self.value = Self::count_descriptors();
        } else {
            self.cached_times -= 1;
        }
        self.value
    }

    fn count_descriptors() -> u64 {
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

fn kb_to_b(kbs: u64) -> u64 {
    kbs * 1024
}
