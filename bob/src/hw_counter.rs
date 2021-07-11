use crate::prelude::*;
use bob_common::metrics::{
    AMOUNT_DESCRIPTORS, CPU_LOAD, FREE_RAM, FREE_SPACE, TOTAL_RAM, TOTAL_SPACE,
};
use std::{fs, path::Path};
use systemstat::{Platform, System};

const DESCRS_DIR: &Path = Path::new("/proc/self/fd/");

pub(crate) struct HWCounter {
    disks: Vec<DiskPath>,
    interval_time: Duration,
    sys: System,
}

impl HWCounter {
    pub(crate) fn new(mapper: Virtual, interval_time: Duration) -> Self {
        Self {
            disks: mapper.local_disks().to_vec(),
            interval_time,
            sys: System::new(),
        }
    }

    pub(crate) fn spawn_task(&self) {
        tokio::spawn(Self::task(self.interval_time, self.disks.clone()));
    }

    async fn task(t: Duration, disks: Vec<DiskPath>) {
        let mut interval = interval(t);
        loop {
            // DelayedMeasurement shouldn't be unwrapped immediately (check in docs)
            let cpu_load = sys.cpu_load_aggregate().expect("Can't get cpu load");
            interval.tick().await;
            gauge!(TOTAL_SPACE, Self::total_space(&disks) as i64);
            gauge!(FREE_SPACE, Self::free_space(&disks) as i64);
            let mem = self.sys.memory().expect("Can't get memory amount");
            gauge!(TOTAL_RAM, mem.total as i64);
            gauge!(FREE_RAM, mem.free as i64);
            gauge!(AMOUNT_DESCRIPTORS, Self::descr_amount() as i64);
            gauge!(CPU_LOAD, cpu_load.done() as i64);
        }
    }

    fn total_space(disks: &[DiskPath]) -> u64 {
        let mut bytes = 0u64;
        for dpath in disks.iter().map(|d| d.path()) {
            bytes += fs2::total_space(dpath).expect("Can't get space info for disk path");
        }
        bytes
    }

    fn free_space(disks: &[DiskPath]) -> u64 {
        let mut bytes = 0u64;
        for dpath in disks.iter().map(|d| d.path()) {
            bytes += fs2::free_space(dpath).expect("Can't get space info for disk path");
        }
        bytes
    }

    fn descr_amount() -> u64 {
        // FIXME: didn't find better way, but iterator's `count` method has O(n) complexity (it may
        // become very slow)
        let d = fs::read_dir(DESCRS_DIR).expect("Can't open proc dir");
        d.count() as u64 - 2 // exclude '.' and '..'
    }
}
