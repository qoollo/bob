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
            backend.cleanup_outdated().await;
        }
    }
}
