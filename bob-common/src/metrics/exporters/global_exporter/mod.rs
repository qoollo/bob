use metrics::{GaugeValue, Key, Recorder};

const NO_RECORDERS_MSG: &str = "\
at least one recorder is expected\
(no reason to build and install global\
recorder without any local exporters)";

mod rate_processor;
use rate_processor::RateProcessor;

pub(crate) struct GlobalRecorder {
    recorders: Vec<Box<dyn Recorder>>,
    rate_processor: RateProcessor,
}

impl GlobalRecorder {
    pub(crate) fn new(recorders: Vec<Box<dyn Recorder>>) -> Self {
        assert!(recorders.len() != 0, "{}", NO_RECORDERS_MSG);
        Self {
            recorders,
            rate_processor: RateProcessor::run_task(),
        }
    }
}

// NOTE: first recorder is processed separately to avoid redundant clone (value is moved)
impl Recorder for GlobalRecorder {
    fn register_gauge(
        &self,
        key: &Key,
        unit: Option<metrics::Unit>,
        description: Option<&'static str>,
    ) {
        for rec in self.recorders.iter() {
            rec.register_gauge(key, unit.clone(), description);
        }
    }

    fn register_counter(
        &self,
        key: &Key,
        unit: Option<metrics::Unit>,
        description: Option<&'static str>,
    ) {
        for rec in self.recorders.iter() {
            rec.register_counter(key, unit.clone(), description);
        }
    }

    fn register_histogram(
        &self,
        key: &Key,
        unit: Option<metrics::Unit>,
        description: Option<&'static str>,
    ) {
        for rec in self.recorders.iter() {
            rec.register_histogram(key, unit.clone(), description);
        }
    }

    fn increment_counter(&self, key: &Key, value: u64) {
        self.rate_processor.process(key, value);
        for rec in self.recorders.iter() {
            rec.increment_counter(key, value);
        }
    }

    #[allow(clippy::cast_sign_loss)]
    fn update_gauge(&self, key: &Key, value: GaugeValue) {
        for rec in self.recorders.iter() {
            rec.update_gauge(key, value.clone());
        }
    }

    fn record_histogram(&self, key: &Key, value: f64) {
        for rec in self.recorders.iter() {
            rec.record_histogram(key, value);
        }
    }
}
