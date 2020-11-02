use super::prelude::*;
use std::time::Duration;

pub(crate) struct Cleaner {
    old_blobs_check_timeout: Duration,
    max_open_blobs: usize,
}

impl Cleaner {
    pub(crate) fn new(old_blobs_check_timeout: Duration, max_open_blobs: usize) -> Self {
        Self {
            old_blobs_check_timeout,
            max_open_blobs,
        }
    }

    pub(crate) fn spawn_task(&self, backend: Arc<Backend>) {
        tokio::spawn(Self::task(
            backend,
            self.old_blobs_check_timeout,
            self.max_open_blobs,
        ));
    }

    async fn task(backend: Arc<Backend>, t: Duration, max_open_blobs: usize) {
        let mut interval = interval(t);
        loop {
            interval.tick().await;
            backend.close_outdated(max_open_blobs).await;
        }
    }
}
