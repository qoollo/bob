const BUFFER_SIZE: usize = 10_485_760; // 10 Mb
use metrics::Key;
use std::time::Duration;
use std::{collections::HashMap, time::Instant};
use tokio::sync::mpsc::{channel, Receiver, Sender};

const RATE_INTERVAL: Duration = Duration::from_secs(5);

type TimeStamp = Instant;

pub(super) struct RateProcessor {
    tx: Sender<(String, u64, TimeStamp)>,
}

impl RateProcessor {
    pub(super) fn run_task() -> Self {
        let (tx, rx) = channel(BUFFER_SIZE);
        tokio::spawn(rate_processor_task(rx));
        Self { tx }
    }

    pub(super) fn process(&self, key: &Key, value: u64) {
        if let Err(e) = self
            .tx
            .try_send((key.name().to_owned(), value, Instant::now()))
        {
            error!("Can't send counter to task with rates (reason: {})", e);
        }
    }
}

async fn rate_processor_task(mut rx: Receiver<(String, u64, TimeStamp)>) {
    let mut counters_map = HashMap::new();
    let mut base_ts = Instant::now();
    if let Some((s, v, t)) = rx.recv().await {
        process_counter(&mut counters_map, s, v);
        base_ts = t;
    }

    loop {
        let val = rx.recv().await;
        if val.is_none() {
            error!("All sender halves for rates are dropped, exiting..");
            return;
        }
        let (k, v, ts) = val.unwrap();
        if ts.duration_since(base_ts) > RATE_INTERVAL {
            base_ts = ts;
            commit_rates(&mut counters_map);
        }
        process_counter(&mut counters_map, k, v);
    }
}

fn process_counter(counters_map: &mut HashMap<String, u64>, k: String, v: u64) {
    let entry = counters_map.entry(k).or_insert_with(|| 0);
    *entry += v;
}

fn commit_rates(counters_map: &mut HashMap<String, u64>) {
    for (k, v) in counters_map.iter_mut() {
        gauge!(format!("{}_rate", k), (*v / RATE_INTERVAL.as_secs()) as f64);
        *v = 0;
    }
}
