extern crate dipstick;
use dipstick::*;
use std::time::Duration;

// metrics! {
//     pub SOME_COUNTER: Counter = "some_counter";
// }

metrics! {
    GRINDER: Proxy = "grinder" => {
        pub GRINDER_PUT_COUNTER: Counter = "put_count";
        pub GRINDER_PUT_ERROR_COUNT_COUNTER: Counter = "put_error_count";
        pub GRINDER_PUT_TIMER: Timer = "put_timer";

        pub GRINDER_GET_COUNTER: Counter = "get_count";
        pub GRINDER_GET_ERROR_COUNT_COUNTER: Counter = "get_error_count";
        pub GRINDER_GET_TIMER: Timer = "get_timer";
    }
}

metrics! {
    CLIENT: Proxy = "client" => {
        pub CLIENT_PUT_COUNTER: Counter = "put_count";
        pub CLINET_PUT_ERROR_COUNT_COUNTER: Counter = "put_error_count";
        pub CLIENT_PUT_TIMER: Timer = "put_timer";

        pub CLIENT_GET_COUNTER: Counter = "get_count";
        pub CLIENT_GET_ERROR_COUNT_COUNTER: Counter = "get_error_count";
        pub CLIENT_GET_TIMER: Timer = "get_timer";
    }
}

#[derive(Clone)]
pub struct BobClientMetrics {
    put_count: Counter,
    put_timer: Timer,
    put_error_count: Counter,

    get_count: Counter,
    get_timer: Timer,
    get_error_count: Counter,
}

impl BobClientMetrics {
    fn new (bucket: AtomicBucket) -> Self {
        BobClientMetrics{ 
            put_count: bucket.clone().counter("put_count"),
            put_timer: bucket.clone().timer("put_timer"),
            put_error_count: bucket.clone().counter("put_error_count"),

            get_count: bucket.clone().counter("get_count"),
            get_timer: bucket.clone().timer("get_timer"),
            get_error_count: bucket.clone().counter("get_error_count"),
        }
    }

    pub(crate) fn put_count(&self) {
        self.put_count.count(1);
    }
    pub(crate) fn put_timer(&self) -> TimeHandle{
        self.put_timer.start()
    }
    pub(crate) fn put_timer_stop(&self, timer: TimeHandle) {
        self.put_timer.stop(timer);
    }
    pub(crate) fn put_error_count(&self) {
        self.put_error_count.count(1);
    }
    
    pub(crate) fn get_count(&self) {
        self.get_count.count(1);
    }
    pub(crate) fn get_timer(&self) -> TimeHandle{
        self.get_timer.start()
    }
    pub(crate) fn get_timer_stop(&self, timer: TimeHandle) {
        self.get_timer.stop(timer);
    }
    pub(crate) fn get_error_count(&self) {
        self.get_error_count.count(1);
    }
}

#[derive(Clone)]
pub struct MetricsContainer<TOutput> {
    output: TOutput,
    duration: Duration,
}

impl<TOutput: Output + Send + Sync + Clone + 'static> MetricsContainer<TOutput> {
    pub fn new(output: TOutput, duration: Duration) -> Self {
        MetricsContainer{
            output,
            duration,
        }
    }

    pub fn get_bucket(self, name: &str) -> BobClientMetrics {
        BobClientMetrics::new(init_bucket(name, self.clone()))
    }
}

pub fn init_counters<TOutput: Output + Send + Sync + Clone + 'static>(prefix: &str, metrics: MetricsContainer<TOutput>) {
    init_grinder(&(prefix.to_owned() + "cluster"), metrics.clone());
    init_bob_client(&(prefix.to_owned() + "backend"), metrics.clone());
}

pub fn init_grinder<TOutput: Output + Send + Sync + Clone + 'static>(prefix: &str, metrics: MetricsContainer<TOutput>) {
    let bucket = init_bucket(prefix, metrics);
    GRINDER.target(bucket.clone());
}

pub fn init_bob_client<TOutput: Output + Send + Sync + Clone + 'static>(prefix: &str, metrics: MetricsContainer<TOutput>) {
    let bucket = init_bucket(prefix, metrics);
    CLIENT.target(bucket);
}

pub fn init_bucket<TOutput: Output + Send + Sync + Clone + 'static>(prefix: &str, metrics: MetricsContainer<TOutput>) -> AtomicBucket
{
    let bucket = AtomicBucket::new().named(prefix.to_string());
    
    bucket.stats(stats_all_bob);
    bucket.drain(metrics.output);

    bucket.flush_every(metrics.duration);
    bucket
}

fn stats_all_bob(
    kind: InputKind,
    name: MetricName,
    score: ScoreType,
) -> Option<(InputKind, MetricName, MetricValue)> {
    match score {
        ScoreType::Count(hit) => Some((InputKind::Counter, name.make_name("count"), hit)),
        ScoreType::Sum(sum) => Some((kind, name.make_name("sum"), sum)),
        ScoreType::Mean(mean) => Some((kind, name.make_name("mean"), mean as MetricValue)),
        ScoreType::Max(max) => Some((InputKind::Gauge, name.make_name("max"), max)),
        ScoreType::Min(min) => Some((InputKind::Gauge, name.make_name("min"), min)),
        ScoreType::Rate(rate) => Some((
            InputKind::Gauge,
            name.make_name("rate"),
            rate as MetricValue,
        )),
    }
}