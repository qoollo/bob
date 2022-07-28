use crate::prelude::*;
use tokio::sync::Mutex;

pub(crate) struct Cleaner {
    old_blobs_check_timeout: Duration,
    soft_open_blobs: Option<usize>,
    hard_open_blobs: Option<usize>,
    bloom_filter_memory_limit: Option<usize>,
    index_memory_limit: Option<usize>,
    index_cleanup_requested: Mutex<bool>,
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
            index_cleanup_requested: Mutex::new(false),
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

    pub(crate) async fn request_index_cleanup(&self) {
        let mut lock = self.index_cleanup_requested.lock().await;
        *lock = true;
    }

    async fn fast_cleaner_task(
        cleaner: Arc<Cleaner>,
        backend: Arc<Backend>,
        t: Duration,
        index_memory_limit: Option<usize>,
    ) {
        let mut interval = interval(t);
        loop {
            interval.tick().await;
            if let Some(limit) = index_memory_limit {
                let mut should_clean_up = false;
                {
                    let mut lock = cleaner.index_cleanup_requested.lock().await;
                    if *lock {
                        *lock = false;
                        should_clean_up = true;
                    }
                }
                if should_clean_up {
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
            cleaner.request_index_cleanup().await;
            if let Some(limit) = bloom_filter_memory_limit {
                backend.offload_old_filters(limit).await;
            }
        }
    }
}
