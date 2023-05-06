use std::{
    collections::BTreeSet,
    sync::atomic::{AtomicUsize, Ordering},
};

use crate::{
    pearl::{holder::PearlSync, Holder},
    prelude::*,
};

#[derive(Clone)]
pub(crate) struct SimpleHolder {
    storage: Arc<RwLock<PearlSync>>,
    timestamp: u64,
}

impl From<&Holder> for SimpleHolder {
    fn from(holder: &Holder) -> Self {
        let storage = holder.cloned_storage();
        let timestamp = holder.end_timestamp();
        Self { storage, timestamp }
    }
}

impl SimpleHolder {
    pub(crate) async fn filter_memory_allocated(&self) -> usize {
        let storage = self.storage.read().await;
        if let Some(storage) = storage.get() {
            storage.filter_memory_allocated().await
        } else {
            0
        }
    }

    pub(crate) async fn offload_filter(&self, needed_memory: usize, level: usize) -> usize {
        let mut storage = self.storage.write().await;
        if let Some(storage) = storage.get_mut() {
            storage.offload_buffer(needed_memory, level).await
        } else {
            0
        }
    }

    pub(crate) fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub(crate) async fn is_ready(&self) -> bool {
        self.storage.read().await.is_ready()
    }
}

impl PartialOrd for SimpleHolder {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.timestamp.partial_cmp(&other.timestamp)
    }
}

impl PartialEq for SimpleHolder {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.storage, &other.storage) && self.timestamp == other.timestamp
    }
}

impl Ord for SimpleHolder {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}

impl Eq for SimpleHolder {}

#[async_trait::async_trait]
pub trait Hooks: Clone {
    async fn storage_prepared(&self, holder: &Holder);
}

#[derive(Clone)]
pub struct NoopHooks;

#[async_trait::async_trait]
impl Hooks for NoopHooks {
    async fn storage_prepared(&self, _holder: &Holder) {}
}

#[derive(Clone, Default)]
pub struct BloomFilterMemoryLimitHooks {
    holders: Arc<RwLock<BTreeSet<SimpleHolder>>>,
    allocated_size: Arc<AtomicUsize>,
    bloom_filter_memory_limit: Option<usize>,
}

impl BloomFilterMemoryLimitHooks {
    pub(crate) fn new(bloom_filter_memory_limit: Option<usize>) -> Self {
        Self {
            bloom_filter_memory_limit,
            ..Default::default()
        }
    }
}

#[async_trait::async_trait]
impl Hooks for BloomFilterMemoryLimitHooks {
    async fn storage_prepared(&self, holder: &Holder) {
        let mut holders = self.holders.write().await;
        let holder: SimpleHolder = holder.into();
        let filter_memory = holder.filter_memory_allocated().await;
        holders.insert(holder);
        self.allocated_size
            .fetch_add(filter_memory, Ordering::Relaxed);
        debug!(
            "Holder added, allocated size: {}",
            self.allocated_size.load(Ordering::Relaxed)
        );
        if let Some(limit) = self.bloom_filter_memory_limit {
            while self.allocated_size.load(Ordering::Relaxed) > limit {
                if let Some(holder) = holders.iter().next().cloned() {
                    // TODO: Better offloading policy at startup
                    let freed = holder.offload_filter(usize::MAX, 0).await;
                    let res = self.allocated_size.fetch_update(
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                        |value| Some(value.saturating_sub(freed)),
                    );
                    debug!(
                        "{} freed, allocated size {}",
                        freed,
                        res.unwrap_or_default()
                    );
                    holders.remove(&holder);
                } else {
                    break;
                }
            }
        }
    }
}
