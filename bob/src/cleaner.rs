use crate::prelude::*;
use tokio::sync::{Mutex, Notify};

pub(crate) struct Cleaner {
    old_blobs_check_timeout: Duration,
    soft_open_blobs: Option<usize>,
    hard_open_blobs: Option<usize>,
    bloom_filter_memory_limit: Option<usize>,
    index_memory_limit: Option<usize>,
    index_cleanup_requested: Notify,
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
            index_cleanup_requested: Notify::new(),
        }
    }

    pub(crate) fn spawn_task(&self, cleaner: Arc<Cleaner>, backend: Arc<Backend>) {
        tokio::spawn(Self::task(
            cleaner.clone(),
            backend.clone(),
            self.old_blobs_check_timeout,
            self.soft_open_blobs,
            self.hard_open_blobs,
            self.bloom_filter_memory_limit,
        ));
        tokio::spawn(Self::fast_cleaner_task(
            cleaner,
            backend,
            Duration::from_secs(30),
            self.index_memory_limit,
        ));
    }

    pub(crate) fn request_index_cleanup(&self) {
        self.index_cleanup_requested.notify_waiters();
    }

    async fn fast_cleaner_task(
        cleaner: Arc<Cleaner>,
        backend: Arc<Backend>,
        t: Duration,
        index_memory_limit: Option<usize>,
    ) {
        if let Some(limit) = index_memory_limit {
            let mut interval = interval(t);
            loop {
                cleaner.index_cleanup_requested.notified().await;
                interval.tick().await;
                let baseline_memory = backend.index_memory().await;
                debug!(
                    "Memory before closing old active blobs: {:?}",
                    baseline_memory
                );
                let mut memory = baseline_memory;
                while memory > limit {
                    if let Some(freed) = backend.close_oldest_active_blob().await {
                        memory = memory - freed;
                        debug!("closed index, freeing {:?} bytes", freed);
                    } else {
                        break;
                    }
                }
                info!(
                    "Memory change closing old active blobs: {:?} -> {:?}",
                    baseline_memory, memory
                );
            }
        }
    }

    async fn task(
        cleaner: Arc<Cleaner>,
        backend: Arc<Backend>,
        t: Duration,
        soft: Option<usize>,
        hard: Option<usize>,
        bloom_filter_memory_limit: Option<usize>,
    ) {
        let mut interval = interval(t);
        loop {
            interval.tick().await;
            if soft.is_some() || hard.is_some() {
                let soft = soft.unwrap_or(1);
                let hard = hard.unwrap_or(10);
                backend.close_unneeded_active_blobs(soft, hard).await;
            }
            cleaner.request_index_cleanup();
            if let Some(limit) = bloom_filter_memory_limit {
                backend.offload_old_filters(limit).await;
            }
        }
    }
}
