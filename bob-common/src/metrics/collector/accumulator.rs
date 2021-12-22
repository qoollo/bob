use super::snapshot::*;
use std::convert::TryInto;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::Receiver;
use tokio::time::{interval, timeout};

const MAX_METRICS_PER_PERIOD: u64 = 100_000;

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
        loop {
            let mut metrics_received = 0;
            let timestamp = get_current_unix_timestamp();
            while let Ok(om) = timeout(self.interval, self.rx.recv()).await {
                match om.map(|m| m.with_timestamp(timestamp)) {
                    Some(Metric::Counter(counter)) => self.snapshot.process_counter(counter),
                    Some(Metric::Gauge(gauge)) => self.snapshot.process_gauge(gauge),
                    Some(Metric::Time(time)) => self.snapshot.process_time(time),
                    // if recv returns None, then sender is dropped, then no more metrics would come
                    None => return,
                }

                metrics_received += 1;
                if metrics_received == MAX_METRICS_PER_PERIOD {
                    break;
                }
            }

            let s = self.snapshot.update_and_get_moment_snapshot();
            *self.readable_snapshot.write().await = s;
        }
    }

    pub(crate) fn get_shared_snapshot(&self) -> SharedMetricsSnapshot {
        self.readable_snapshot.clone()
    }
}

fn get_current_unix_timestamp() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(n) => n.as_secs().try_into().expect("timestamp conversion"),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}
