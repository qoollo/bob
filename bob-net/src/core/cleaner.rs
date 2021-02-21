use crate::prelude::*;

pub(crate) struct Cleaner {
    old_blobs_check_timeout: Duration,
    soft_open_blobs: usize,
    hard_open_blobs: usize,
}

impl Cleaner {
    pub(crate) fn new(
        old_blobs_check_timeout: Duration,
        soft_open_blobs: usize,
        hard_open_blobs: usize,
    ) -> Self {
        Self {
            old_blobs_check_timeout,
            soft_open_blobs,
            hard_open_blobs,
        }
    }

    pub(crate) fn spawn_task(&self, backend: Arc<Backend>) {
        tokio::spawn(Self::task(
            backend,
            self.old_blobs_check_timeout,
            self.soft_open_blobs,
            self.hard_open_blobs,
        ));
    }

    async fn task(backend: Arc<Backend>, t: Duration, soft: usize, hard: usize) {
        let mut interval = interval(t);
        loop {
            interval.tick().await;
            backend.close_unneeded_active_blobs(soft, hard).await;
        }
    }
}
