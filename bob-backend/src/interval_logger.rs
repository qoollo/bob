use coarsetime::{Duration as CDuration, Instant as CInstant};
use std::{
    collections::HashMap,
    hash::Hash,
    cmp::Eq,
    fmt::Display
};

#[derive(Debug)]
pub struct IntervalErrorLogger<E> {
    errors: HashMap<E, u64>,
    interval: CDuration,
    last_timestamp: CInstant,
}

impl<E: Hash + Eq + Display> IntervalErrorLogger<E> {
    pub fn new(interval_ms: u64) -> Self {
        Self {
            errors: HashMap::new(),
            interval: CDuration::from_millis(interval_ms),
            last_timestamp: CInstant::now(),
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
                    error!("{} [{} times]", action, count);
                    *count = 0;
                }
            }
            self.last_timestamp = CInstant::now();
        }
    }
}