extern crate dipstick;
use crate::core::configs::node::NodeConfig;
use crate::core::backend::pearl::metrics::init_pearl;

use dipstick::*;
use std::{sync::Arc, time::Duration};

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
    fn new(bucket: AtomicBucket) -> Self {
        BobClientMetrics {
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
    pub(crate) fn put_timer(&self) -> TimeHandle {
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
    pub(crate) fn get_timer(&self) -> TimeHandle {
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
struct MetricsContainer<TOutput> {
    output: TOutput,
    duration: Duration,

    prefix: String,
}
impl<TOutput: Output + Send + Sync + Clone + 'static> MetricsContainer<TOutput> {
    pub fn new(output: TOutput, duration: Duration, prefix: &str) -> Self {
        MetricsContainer {
            output,
            duration,
            prefix: prefix.to_string(),
        }
    }
}

pub trait MetricsContainerBuilder {
    fn get_metrics(&self, name: &str) -> BobClientMetrics;
    fn init_bucket(&self, prefix: &str) -> AtomicBucket;
}
impl<TOutput: Output + Send + Sync + Clone + 'static> MetricsContainerBuilder
    for MetricsContainer<TOutput>
{
    fn get_metrics(&self, name: &str) -> BobClientMetrics {
        let prefix = self.prefix.clone() + ".to." + name;
        BobClientMetrics::new(self.init_bucket(&prefix))
    }

    fn init_bucket(&self, prefix: &str) -> AtomicBucket {
        let bucket = AtomicBucket::new().named(prefix.to_string());

        bucket.stats(stats_all_bob);
        bucket.drain(self.output.clone());

        bucket.flush_every(self.duration);
        bucket
    }
}

//////  init counters

fn prepare_metrics_addres(address: String) -> String {
    address.replace(".", "_") + "."
}

fn default_metrics() -> Arc<dyn MetricsContainerBuilder + Send + Sync> {
    Arc::new(MetricsContainer::new(
        Void::new(),
        Duration::from_secs(100000),
        &"",
    ))
}

pub fn init_counters(node_config: &NodeConfig) -> Arc<dyn MetricsContainerBuilder + Send + Sync> {
    let prefix = prepare_metrics_addres(node_config.bind());

    let mut metrics: Arc<dyn MetricsContainerBuilder + Send + Sync> = default_metrics();
    if let Some(config) = &node_config.metrics {
        if let Some(graphite) = &config.graphite {
            let mut gr = Graphite::send_to(graphite).expect("cannot init metrics for Graphite");;
            if let Some(name) = &config.name {
                gr = gr.named(name);
            }
            metrics = Arc::new(MetricsContainer::new(gr, Duration::from_secs(1), &prefix));
        }
    }

    init_grinder(&(prefix.to_owned() + "cluster"), &metrics);
    init_bob_client(&(prefix.to_owned() + "backend"), &metrics);
    init_pearl(&(prefix.to_owned() + "pearl"), &metrics);

    metrics
}

fn init_grinder(prefix: &str, metrics: &Arc<dyn MetricsContainerBuilder + Send + Sync>) {
    let bucket = metrics.init_bucket(prefix);
    GRINDER.target(bucket.clone());
}

fn init_bob_client(prefix: &str, metrics: &Arc<dyn MetricsContainerBuilder + Send + Sync>) {
    let bucket = metrics.init_bucket(prefix);
    CLIENT.target(bucket);
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
