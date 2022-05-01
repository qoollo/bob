use coarsetime::{Duration as CDuration, Instant as CInstant};
use std::{
    collections::HashMap,
    hash::Hash,
    cmp::Eq,
    fmt::Display,
    sync::Mutex,
};
use log::Level;

#[derive(Debug)]
pub struct IntervalLogger<E> {
    logs: HashMap<E, u64>,
    interval: CDuration,
    last_timestamp: CInstant,
    level: Level,
}

impl<E: Hash + Eq + Display> IntervalLogger<E> {
    pub fn new(interval_ms: u64, level: Level) -> Self {
        let interval = CDuration::from_millis(interval_ms);
        Self {
            logs: HashMap::new(),
            interval,
            last_timestamp: CInstant::now() - interval - CDuration::from_millis(1),
            level,
        }
    }

    pub fn report(&mut self, action: E) {
        if let Some(count) = self.logs.get_mut(&action) {
            *count += 1;
        } else {
            self.logs.insert(action, 1);
        }

        if self.last_timestamp.elapsed() > self.interval {
            for (action, count) in self.logs.iter_mut() {
                if *count > 0 {
                    log!(self.level, "{} [{} times]", action, count);
                    *count = 0;
                }
            }
            self.last_timestamp = CInstant::now();
        }
    }
}

#[derive(Debug)]
pub struct IntervalLoggerSafe<E> {
    inner: Mutex<IntervalLogger<E>>
}

impl<E: Hash + Eq + Display> IntervalLoggerSafe<E> {
    pub fn new(interval_ms: u64, level: Level) -> Self {
        Self {
            inner: Mutex::new(IntervalLogger::new(interval_ms, level))
        }
    }

    pub fn report(&self, action: E) {
        if let Ok(mut lock) = self.inner.lock() {
            lock.report(action);
        }
    }
}