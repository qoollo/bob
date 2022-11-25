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
    errors: HashMap<E, u64>,
    interval: CDuration,
    last_timestamp: CInstant,
    level: Level,
}

impl<E: Hash + Eq + Display> IntervalLogger<E> {
    pub fn new(interval_ms: u64, level: Level) -> Self {
        let interval = CDuration::from_millis(interval_ms);
        Self {
            errors: HashMap::new(),
            interval,
            last_timestamp: CInstant::now() - interval - CDuration::from_millis(1),
            level,
        }
    }

    pub fn report_error(&mut self, action: E) {
        if let Some(count) = self.errors.get_mut(&action) {
            *count += 1;
        } else {
            self.errors.insert(action, 1);
        }

        if self.last_timestamp.elapsed() > self.interval {
            for (action, count) in self.errors.iter_mut() {
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

    pub fn report_error(&self, action: E) {
        self.inner.lock().expect("interval logger mutex").report_error(action);
    }
}
