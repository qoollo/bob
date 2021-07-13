use crate::prelude::*;
use bob_common::metrics::{
    AMOUNT_DESCRIPTORS, CPU_LOAD, FREE_RAM, FREE_SPACE, TOTAL_RAM, TOTAL_SPACE,
};
use std::path::{Path, PathBuf};
use sysinfo::{DiskExt, ProcessExt, System, SystemExt};

const DESCRS_DIR: &str = "/proc/self/fd/";

pub(crate) struct HWCounter {
    disks: HashMap<PathBuf, String>,
    interval_time: Duration,
}

impl HWCounter {
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
                        let dpath = Path::new(dp.path());
                        dpath.starts_with(&path)
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
        let pid = std::process::id() as i32;

        loop {
            interval.tick().await;
            sys.refresh_all();
            sys.refresh_disks();
            let proc = sys.process(pid).expect("Can't get process stat descriptor");

            let (total_space, free_space) = Self::space(&sys, &disks);
            gauge!(TOTAL_SPACE, total_space as i64);
            gauge!(FREE_SPACE, free_space as i64);
            let used_mem = sys.used_memory() / 1024; // in Mb
            let free_mem = sys.free_memory() / 1024; // in Mb
            gauge!(TOTAL_RAM, used_mem as i64);
            gauge!(FREE_RAM, free_mem as i64);
            gauge!(AMOUNT_DESCRIPTORS, Self::descr_amount() as i64);
            gauge!(CPU_LOAD, proc.cpu_usage() as i64);
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
                let disk_total = disk.total_space();
                let disk_free = disk.available_space();
                println!(
                    "{} (with path {}): total = {}, free = {};",
                    diskname,
                    disk.mount_point()
                        .to_str()
                        .expect("Can't parse mount point"),
                    disk_total,
                    disk_free
                );
                (total + disk_total / 1024, free + disk_free / 1024) // in Mb
            })
    }

    fn descr_amount() -> u64 {
        // FIXME: didn't find better way, but iterator's `count` method has O(n) complexity (it may
        // become very slow)
        let d = std::fs::read_dir(DESCRS_DIR).expect("Can't open proc dir");
        d.count() as u64 - 2 // exclude '.' and '..'
    }
}
