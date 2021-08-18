use std::time::Duration;

use tokio::sync::mpsc::channel;

use self::accumulator::MetricsAccumulator;

pub mod accumulator;
pub mod recorder;
pub mod snapshot;

pub(crate) use self::recorder::MetricsRecorder;
pub use self::snapshot::SharedMetricsSnapshot;

pub(crate) fn establish_global_collector(
    check_interval: Duration,
) -> (MetricsRecorder, SharedMetricsSnapshot) {
    const BUFFER_SIZE: usize = 1_048_576; // 1 Mb

    let (tx, rx) = channel(BUFFER_SIZE);
    let recorder = MetricsRecorder::new(tx);
    let accumulator = MetricsAccumulator::new(rx, check_interval);
    let metrics = accumulator.get_shared_snapshot();
    tokio::spawn(accumulator.run());
    (recorder, metrics)
}
