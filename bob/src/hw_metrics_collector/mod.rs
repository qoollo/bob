use crate::{prelude::*, hw_metrics_collector::os_data_fetcher::OsDataFetcher};
use bob_common::metrics::{
    BOB_RAM, BOB_VIRTUAL_RAM, BOB_CPU_LOAD, DESCRIPTORS_AMOUNT, CPU_IOWAIT, 
    TOTAL_RAM, AVAILABLE_RAM, USED_RAM, USED_SWAP,
    TOTAL_SPACE, FREE_SPACE, USED_SPACE, HW_DISKS_FOLDER
};
use sysinfo::{ProcessExt, ProcessRefreshKind, RefreshKind, System, SystemExt};

mod os_data_fetcher;

#[derive(Default)]
pub(crate) struct DiskSpaceMetrics {
    pub(crate) total_space: u64,
    pub(crate) used_space: u64,
    pub(crate) free_space: u64,
}

pub(crate) struct HWMetricsCollector {
    os_metrics_collector: OsDataFetcher,
    interval_time: Duration,
}

impl HWMetricsCollector {
    pub(crate) async fn new(mapper: Arc<Virtual>, interval_time: Duration) -> Self {
        let os_metrics_collector = OsDataFetcher::new(mapper.local_disks()).await;
        Self {
            os_metrics_collector,
            interval_time,
        }
    }

    pub(crate) fn spawn_task(&self) {
        tokio::spawn(Self::task(self.interval_time, self.os_metrics_collector.clone()));
    }

    pub(crate) async fn update_space_metrics(&self) -> DiskSpaceMetrics {
        Self::update_space_metrics_from_disks(&self.os_metrics_collector).await
    }

    async fn task(t: Duration, mut os_metrics_collector: OsDataFetcher) {
        let mut interval = interval(t);
        let mut sys = System::new_all();
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

            if let Some(proc) = sys.process(pid) {
                gauge!(BOB_CPU_LOAD, proc.cpu_usage() as f64);
                let bob_ram = proc.memory();
                gauge!(BOB_RAM, bob_ram as f64);
                let bob_virtual_ram = proc.virtual_memory();
                gauge!(BOB_VIRTUAL_RAM, bob_virtual_ram as f64);
            } else {
                debug!("Can't get process stat descriptor");
            }

            let available_mem = sys.available_memory();
            let used_mem = total_mem - available_mem;
            let used_swap = sys.used_swap();
            debug!("used mem in bytes: {} | available mem in bytes: {} | used swap: {}", used_mem, available_mem, used_swap);
            gauge!(USED_RAM, used_mem as f64);
            gauge!(AVAILABLE_RAM, available_mem as f64);
            gauge!(USED_SWAP, used_swap as f64);

            let data = os_metrics_collector.collect().await;

            let _ = Self::update_space_metrics_from_disks(&os_metrics_collector);

            if let Some(descr_amount) = data.descriptors_amount {
                gauge!(DESCRIPTORS_AMOUNT, descr_amount);
            }

            if let Some(cpu_iowait) = data.cpu_iowait {
                gauge!(CPU_IOWAIT, cpu_iowait);
            }

            for (disk_id, metrics) in data.disk_metrics {
                if let Some(iops) = metrics.iops {
                    gauge!(format!("{}.{}_iops", HW_DISKS_FOLDER, disk_id), iops);
                }
                if let Some(util) = metrics.util {
                    gauge!(format!("{}.{}_util", HW_DISKS_FOLDER, disk_id), util);
                }
            }
        }
    }

    async fn update_space_metrics_from_disks(os_metrics_collector: &OsDataFetcher) -> DiskSpaceMetrics {
        let disks_metrics = os_metrics_collector.collect_space_metrics().await;
        gauge!(TOTAL_SPACE, bytes_to_mb(disks_metrics.total_space) as f64);
        gauge!(USED_SPACE, bytes_to_mb(disks_metrics.used_space) as f64);
        gauge!(FREE_SPACE, bytes_to_mb(disks_metrics.free_space) as f64);
        disks_metrics
    }

}

fn bytes_to_mb(bytes: u64) -> u64 {
    bytes / 1024 / 1024
}
