use crate::prelude::*;
use bob_common::metrics::ACTIVE_DISKS_COUNT;

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
        loop {
            interval.tick().await;
            let (normal_blobs, alien_blobs) = backend.blobs_count().await;
            gauge!(BLOBS_COUNT, normal_blobs as i64);
            gauge!(ALIEN_BLOBS_COUNT, alien_blobs as i64);
            let active_disks = backend.active_disks_count().await;
            gauge!(ACTIVE_DISKS_COUNT, active_disks as i64);
            let index_memory = backend.index_memory().await;
            gauge!(INDEX_MEMORY, index_memory as i64);
        }
    }
}
