use crate::prelude::*;
use bob_common::metrics::{ACTIVE_DISKS_COUNT, BLOOM_FILTERS_RAM, DISK_USED};

pub(crate) struct Counter {
    count_interval: Duration,
}

impl Counter {
    pub(crate) fn new(count_interval: Duration) -> Self {
        Self { count_interval }
    }

    pub(crate) fn spawn_task(&self, backend: Arc<Backend>) {
        tokio::spawn(Self::task(backend, self.count_interval));
    }

    async fn task(backend: Arc<Backend>, t: Duration) {
        let mut interval = interval(t);
        let mut cached_normal_blobs = 0;
        let mut cached_alien_blobs = 0;
        loop {
            debug!("update blobs count metrics");
            interval.tick().await;
            let (normal_blobs, alien_blobs) = backend.blobs_count().await;
            gauge!(BLOBS_COUNT, normal_blobs as f64);
            gauge!(ALIEN_BLOBS_COUNT, alien_blobs as f64);
            let active_disks = backend.active_disks_count().await;
            gauge!(ACTIVE_DISKS_COUNT, active_disks as f64);
            let index_memory = backend.index_memory().await;
            gauge!(INDEX_MEMORY, index_memory as f64);
            let disk_used = backend.disk_used_by_disk().await;
            gauge!(DISK_USED, disk_used.values().sum::<u64>() as f64);

            for (disk, used) in disk_used {
                gauge!(format!("{}.{}", DISK_USED, disk.name()), used as f64);
            }

            if normal_blobs != cached_normal_blobs || alien_blobs != cached_alien_blobs {
                let bf_ram = backend.filter_memory_allocated().await;
                gauge!(BLOOM_FILTERS_RAM, bf_ram as f64);
                cached_normal_blobs = normal_blobs;
                cached_alien_blobs = alien_blobs;
            }
        }
    }
}
