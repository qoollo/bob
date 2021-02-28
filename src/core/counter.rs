use super::prelude::*;
use std::time::Duration;

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
            let metrics = backend.collect_metrics().await;
            gauge!(ACTIVE_DISKS_COUNT, metrics.active_disks_cnt as i64);
            gauge!(BLOBS_COUNT, metrics.blobs_cnt as i64);
            gauge!(ALIEN_BLOBS_COUNT, metrics.aliens_cnt as i64);
            gauge!(INDEX_MEMORY, metrics.index_memory as i64);
        }
    }
}
