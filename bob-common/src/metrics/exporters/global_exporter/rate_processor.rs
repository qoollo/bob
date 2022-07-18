const BUFFER_SIZE: usize = 10_000_000;
use metrics::Key;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::{collections::HashMap, time::Instant};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{channel, Receiver, Sender};

const RATE_INTERVAL: Duration = Duration::from_secs(5);

const IS_ACTIVE_STORE_ORDERING: Ordering = Ordering::Release;
const IS_ACTIVE_LOAD_ORDERING: Ordering = Ordering::Acquire;

type TimeStamp = Instant;

#[derive(Debug)]
struct RateMessage {
    counter_name: String,
    counter_inc_value: u64,
    timestamp: TimeStamp,
}

impl RateMessage {
    fn new(counter_name: String, counter_inc_value: u64, timestamp: TimeStamp) -> Self {
        Self {
            counter_name,
            counter_inc_value,
            timestamp,
        }
    }

    fn unpack(self) -> (String, u64, TimeStamp) {
        (self.counter_name, self.counter_inc_value, self.timestamp)
    }
}

pub(super) struct RateProcessor {
    tx: Sender<RateMessage>,
    is_active: AtomicBool,
}

impl RateProcessor {
    pub(super) fn run_task() -> Self {
        let (tx, rx) = channel(BUFFER_SIZE);
        tokio::spawn(rate_processor_task(rx));
        let is_active = AtomicBool::new(true);
        Self { tx, is_active }
    }

    pub(super) fn process(&self, key: &Key, value: u64) {
        if self.is_active.load(IS_ACTIVE_LOAD_ORDERING) {
            if let Err(e) = self.tx.try_send(RateMessage::new(
                key.name().to_owned(),
                value,
                Instant::now(),
            )) {
                match e {
                    TrySendError::Full(msg) => {
                        error!("Rate processor buffer is full, {:?} is not written", msg);
                    }
                    TrySendError::Closed(_) => {
                        error!("Rate processor task dropped, making it inactive..");
                        self.is_active.store(false, IS_ACTIVE_STORE_ORDERING);
                    }
                }
            }
        }
    }
}

async fn rate_processor_task(mut rx: Receiver<RateMessage>) {
    let mut counters_map = HashMap::new();
    let mut base_ts = Instant::now();
    if let Some((s, v, t)) = rx.recv().await.map(|rm| rm.unpack()) {
        process_counter(&mut counters_map, s, v);
        base_ts = t;
    }

    while let Some(val) = rx.recv().await {
        let (k, v, ts) = val.unpack();
        if ts > base_ts && ts.duration_since(base_ts) > RATE_INTERVAL {
            base_ts = ts;
            commit_rates(&mut counters_map);
        }
        process_counter(&mut counters_map, k, v);
    }
    error!("All sender halves for rates are dropped, exiting..");
}

fn process_counter(counters_map: &mut HashMap<String, u64>, k: String, v: u64) {
    let entry = counters_map.entry(k).or_insert(0);
    *entry += v;
}

fn commit_rates(counters_map: &mut HashMap<String, u64>) {
    for (k, v) in counters_map.iter_mut() {
        gauge!(format!("{}_rate", k), (*v / RATE_INTERVAL.as_secs()) as f64);
        *v = 0;
    }
}
