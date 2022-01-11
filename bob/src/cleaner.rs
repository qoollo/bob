use crate::prelude::*;

pub(crate) struct Cleaner {
    old_blobs_check_timeout: Duration,
    soft_open_blobs: usize,
    hard_open_blobs: usize,
    bloom_filter_memory_limit: Option<usize>,
}

impl Cleaner {
    pub(crate) fn new(
        old_blobs_check_timeout: Duration,
        soft_open_blobs: usize,
        hard_open_blobs: usize,
        bloom_filter_memory_limit: Option<usize>,
    ) -> Self {
        Self {
            old_blobs_check_timeout,
            soft_open_blobs,
            hard_open_blobs,
            bloom_filter_memory_limit,
        }
    }

    pub(crate) fn spawn_task(&self, backend: Arc<Backend>) {
        tokio::spawn(Self::task(
            backend,
            self.old_blobs_check_timeout,
            self.soft_open_blobs,
            self.hard_open_blobs,
            self.bloom_filter_memory_limit,
        ));
    }

    async fn task(
        backend: Arc<Backend>,
        t: Duration,
        soft: usize,
        hard: usize,
        bloom_filter_memory_limit: Option<usize>,
    ) {
        let mut interval = interval(t);
        loop {
            interval.tick().await;
            backend.close_unneeded_active_blobs(soft, hard).await;
            if let Some(limit) = bloom_filter_memory_limit {
                backend.offload_old_filters(limit).await;
            }
        }
    }
}
