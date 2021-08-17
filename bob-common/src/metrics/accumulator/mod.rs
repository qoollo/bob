use super::snapshot::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::time::{interval, timeout};

const METRICS_RECV_TIMEOUT: Duration = Duration::from_millis(100);

pub(crate) struct MetricsAccumulator {
    rx: Receiver<Metric>,
    interval: Duration,
    snapshot: MetricsSnapshot,
    readable_snapshot: SharedMetricsSnapshot,
}

impl MetricsAccumulator {
    pub(super) fn new(rx: Receiver<Metric>, interval: Duration) -> Self {
        Self {
            rx,
            interval,
            snapshot: MetricsSnapshot::default(),
            readable_snapshot: Arc::default(),
        }
    }
    // this function runs in other thread, so it would be better if it will take control of arguments
    // themselves, not just references
    #[allow(clippy::needless_pass_by_value)]
    pub(crate) async fn run(mut self) {
        let mut interval = interval(self.interval);

        loop {
            interval.tick().await;
            while let Ok(m) = timeout(METRICS_RECV_TIMEOUT, self.rx.recv()).await {
                match m {
                    Some(Metric::Counter(counter)) => self.snapshot.process_counter(counter),
                    Some(Metric::Gauge(gauge)) => self.snapshot.process_gauge(gauge),
                    Some(Metric::Time(time)) => self.snapshot.process_time(time),
                    // if recv returns None, then sender is dropped, then no more metrics would come
                    None => return,
                }
            }

            let mut w = self.readable_snapshot.write().await;
            *w = self.snapshot.update_and_get_moment_snapshot();
        }
    }

    pub(crate) fn get_shared_snapshot(&self) -> SharedMetricsSnapshot {
        self.readable_snapshot.clone()
    }
}
