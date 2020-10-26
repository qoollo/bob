use super::prelude::*;
use std::time::Duration;

pub(crate) struct Cleaner {
    old_blobs_check_timeout: Duration,
}

impl Cleaner {
    pub(crate) fn new(old_blobs_check_timeout: Duration) -> Self {
        Self {
            old_blobs_check_timeout,
        }
    }

    pub(crate) fn spawn_task(&self, backend: Arc<Backend>) {
        tokio::spawn(Self::task(backend, self.old_blobs_check_timeout));
    }

    async fn task(backend: Arc<Backend>, t: Duration) {
        let mut interval = interval(t);
        loop {
            interval.tick().await;
            let groups = backend.inner().vdisks_groups();
            if let Some(groups) = groups {
                for group in groups {
                    let holders_lock = group.holders();
                    let mut holders_write = holders_lock.write().await;
                    let holders: &mut Vec<_> = holders_write.as_mut();
                    let old_holders: Vec<_> = holders.drain_filter(|h| h.is_outdated()).collect();
                    for mut holder in old_holders {
                        holder.free();
                    }
                }
            }
        }
    }
}
