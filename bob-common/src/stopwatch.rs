use std::default::Default;
use std::fmt;
use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
pub struct Stopwatch {
    state: StopwatchState,
}

#[derive(Clone, Copy)]
enum StopwatchState {
    NotStarted,
    Started(Instant),
    Stopped(Duration),
}

impl Default for Stopwatch {
    fn default() -> Stopwatch {
        Stopwatch {
            state: StopwatchState::NotStarted,
        }
    }
}

impl fmt::Display for Stopwatch {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        return write!(f, "{}ms", self.elapsed_ms());
    }
}

impl Stopwatch {
    /// Returns a new stopwatch.
    pub fn new() -> Stopwatch {
        Self::default()
    }

    /// Returns a new stopwatch which will immediately be started.
    pub fn start_new() -> Stopwatch {
        let mut sw = Stopwatch::new();
        sw.start();
        return sw;
    }

    /// Starts the stopwatch.
    pub fn start(&mut self) {
        self.state = StopwatchState::Started(Instant::now());
    }

    /// Stops the stopwatch.
    pub fn stop(&mut self) {
        self.state = StopwatchState::Stopped(self.elapsed());
    }

    /// Resets all counters and stops the stopwatch.
    pub fn reset(&mut self) {
        self.state = StopwatchState::NotStarted;
    }

    /// Resets and starts the stopwatch again.
    pub fn restart(&mut self) {
        self.reset();
        self.start();
    }

    /// Returns whether the stopwatch is running.
    pub fn is_running(&self) -> bool {
        matches!(self.state, StopwatchState::Started(_))
    }

    /// Returns the elapsed time since the start of the stopwatch.
    pub fn elapsed(&self) -> Duration {
        match self.state {
            StopwatchState::NotStarted => Duration::ZERO,
            StopwatchState::Started(i) => i.elapsed(),
            StopwatchState::Stopped(d) => d,
        }
    }

    /// Returns the elapsed time since the start of the stopwatch in milliseconds.
    pub fn elapsed_ms(&self) -> u64 {
        let dur = self.elapsed();
        dur.as_millis() as u64
    }
}
