use crate::prelude::*;

pub(crate) struct Cleaner {
    old_blobs_check_timeout: Duration,
    soft_open_blobs: Option<usize>,
    hard_open_blobs: Option<usize>,
    bloom_filter_memory_limit: Option<usize>,
    index_memory_limit: Option<usize>,
}

impl Cleaner {
    pub(crate) fn new(
        old_blobs_check_timeout: Duration,
        soft_open_blobs: Option<usize>,
        hard_open_blobs: Option<usize>,
        bloom_filter_memory_limit: Option<usize>,
        index_memory_limit: Option<usize>,
    ) -> Self {
        Self {
            old_blobs_check_timeout,
            soft_open_blobs,
            hard_open_blobs,
            bloom_filter_memory_limit,
            index_memory_limit,
        }
    }

    pub(crate) fn spawn_task(&self, backend: Arc<Backend>) {
        tokio::spawn(Self::task(
            backend,
            self.old_blobs_check_timeout,
            self.soft_open_blobs,
            self.hard_open_blobs,
            self.bloom_filter_memory_limit,
            self.index_memory_limit,
        ));
    }

    async fn task(
        backend: Arc<Backend>,
        t: Duration,
        soft: Option<usize>,
        hard: Option<usize>,
        bloom_filter_memory_limit: Option<usize>,
        index_memory_limit: Option<usize>,
    ) {
        let mut interval = interval(t);
        loop {
            interval.tick().await;
            if soft.is_some() || hard.is_some() {
                let soft = soft.unwrap_or(1);
                let hard = hard.unwrap_or(10);
                backend.close_unneeded_active_blobs(soft, hard).await;
            }
            if let Some(limit) = bloom_filter_memory_limit {
                backend.offload_old_filters(limit).await;
            }
            if let Some(limit) = index_memory_limit {
                let mut memory = backend.index_memory().await;
                info!("Memory before closing old active blobs: {:?}", memory);
                while memory > limit {
                    if let Some(freed) = backend.close_oldest_active_blob().await {
                        memory = memory - freed;
                        debug!("closed index, freeing {:?} bytes", freed);
                    } else {
                        break;
                    }
                }
                info!("Memory after closing old active blobs: {:?}", memory);
            }
        }
    }
}
