use crate::prelude::*;
use tokio::sync::{Mutex, Notify};

pub(crate) struct Cleaner {
    old_blobs_check_timeout: Duration,
    soft_open_blobs: Option<usize>,
    hard_open_blobs: Option<usize>,
    bloom_filter_memory_limit: Option<usize>,
    index_memory_limit: Option<usize>,
    index_memory_limit_soft: Option<usize>,
    index_cleanup_notification: Notify,
    cleaning_lock: Mutex<()>,
}

impl Cleaner {
    pub(crate) fn new(
        old_blobs_check_timeout: Duration,
        soft_open_blobs: Option<usize>,
        hard_open_blobs: Option<usize>,
        bloom_filter_memory_limit: Option<usize>,
        index_memory_limit: Option<usize>,
        index_memory_limit_soft: Option<usize>,
    ) -> Self {
        Self {
            old_blobs_check_timeout,
            soft_open_blobs,
            hard_open_blobs,
            bloom_filter_memory_limit,
            index_memory_limit,
            index_memory_limit_soft: index_memory_limit_soft
                .or(index_memory_limit.map(|l| l * 10 / 9)),
            index_cleanup_notification: Notify::new(),
            cleaning_lock: Mutex::new(()),
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
            self.index_memory_limit_soft,
        ));
        tokio::spawn(Self::fast_cleaner_task(
            cleaner,
            backend,
            self.index_memory_limit,
        ));
    }

    pub(crate) fn request_index_cleanup(&self) {
        self.index_cleanup_notification.notify_waiters();
    }

    async fn fast_cleaner_task(
        cleaner: Arc<Cleaner>,
        backend: Arc<Backend>,
        index_memory_limit: Option<usize>,
    ) {
        if let Some(limit) = index_memory_limit {
            let mut interval = interval(Duration::from_secs(5));
            loop {
                cleaner.index_cleanup_notification.notified().await;
                interval.tick().await;
                let _lck = cleaner.cleaning_lock.lock().await;
                let baseline_memory = backend.index_memory().await;
                let mut memory = baseline_memory;
                while memory > limit {
                    if let Some(freed) = backend.free_least_used_holder_resources().await {
                        memory = memory - freed;
                        debug!("freed resources, {:?} bytes", freed);
                    } else {
                        break;
                    }
                }
                info!(
                    "Memory change freeing resources: {:?} -> {:?}",
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
        index_memory_limit: Option<usize>,
    ) {
        let mut interval = interval(t);
        loop {
            interval.tick().await;
            if soft.is_some() || hard.is_some() {
                let soft = soft.unwrap_or(1);
                let hard = hard.unwrap_or(10);
                let _lck = cleaner.cleaning_lock.lock().await;
                index_cleanup_by_active_count_limit(&backend, soft, hard).await;
            }
            if let Some(limit) = index_memory_limit {
                let _lck = cleaner.cleaning_lock.lock().await;
                index_cleanup_by_memory_limit(&backend, limit).await;
            }
            if let Some(limit) = bloom_filter_memory_limit {
                backend.offload_old_filters(limit).await;
            }
        }
    }
}

async fn index_cleanup_by_active_count_limit(backend: &Arc<Backend>, soft: usize, hard: usize) {
    backend.close_unneeded_active_blobs(soft, hard).await;
}

async fn index_cleanup_by_memory_limit(backend: &Arc<Backend>, limit: usize) {
    let baseline_memory = backend.index_memory().await;
    debug!(
        "Memory before closing old active blob indexes: {:?}",
        baseline_memory
    );
    let mut memory = baseline_memory;
    while memory > limit {
        if let Some(freed) = backend.close_oldest_active_blob().await {
            memory = memory - freed;
            debug!("closed index for active blob, freeing {:?} bytes", freed);
        } else {
            break;
        }
    }
    info!(
        "Memory change closing old active blob indexes: {:?} -> {:?}",
        baseline_memory, memory
    );
}
