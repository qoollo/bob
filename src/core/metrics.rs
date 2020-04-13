use super::prelude::*;

metrics! {
    GRINDER: Proxy = "grinder" => {
        /// Counts number of PUT requests, processed by Grinder
        pub GRINDER_PUT_COUNTER: Counter = "put_count";
        /// Counts number of PUT requests return error, processed by Grinder
        pub GRINDER_PUT_ERROR_COUNT_COUNTER: Counter = "put_error_count";
        /// Measures processing time of the PUT request
        pub GRINDER_PUT_TIMER: Timer = "put_timer";

        /// Counts number of GET requests, processed by Grinder
        pub GRINDER_GET_COUNTER: Counter = "get_count";
        /// Counts number of GET requests return error, processed by Grinder
        pub GRINDER_GET_ERROR_COUNT_COUNTER: Counter = "get_error_count";
        /// Measures processing time of the GET request
        pub GRINDER_GET_TIMER: Timer = "get_timer";
    }
}

metrics! {
    CLIENT: Proxy = "client" => {
        /// Counts number of PUT requests, processed by Client
        pub CLIENT_PUT_COUNTER: Counter = "put_count";
        /// Counts number of PUT requests return error, processed by Client
        pub CLIENT_PUT_ERROR_COUNT_COUNTER: Counter = "put_error_count";
        /// Measures processing time of the PUT request
        pub CLIENT_PUT_TIMER: Timer = "put_timer";
        /// Counts number of GET requests, processed by Client

        pub CLIENT_GET_COUNTER: Counter = "get_count";
        /// Counts number of GET requests return error, processed by Client
        pub CLIENT_GET_ERROR_COUNT_COUNTER: Counter = "get_error_count";
        /// Measures processing time of the GET request
        pub CLIENT_GET_TIMER: Timer = "get_timer";
    }
}

/// Structure contains put/get metrics for `BobClient`
#[derive(Debug, Clone)]
pub struct BobClient {
    put_count: Counter,
    put_timer: Timer,
    put_error_count: Counter,
    get_count: Counter,
    get_timer: Timer,
    get_error_count: Counter,
}

impl BobClient {
    fn new(bucket: &AtomicBucket) -> Self {
        BobClient {
            put_count: bucket.counter("put_count"),
            put_timer: bucket.timer("put_timer"),
            put_error_count: bucket.counter("put_error_count"),
            get_count: bucket.counter("get_count"),
            get_timer: bucket.timer("get_timer"),
            get_error_count: bucket.counter("get_error_count"),
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

#[derive(Debug, Clone)]
struct MetricsContainer<T> {
    output: T,
    duration: Duration,
    prefix: String,
}

impl<T: Output> MetricsContainer<T> {
    pub(crate) fn new(output: T, duration: Duration, prefix: String) -> Self {
        MetricsContainer {
            output,
            duration,
            prefix,
        }
    }
}

/// A trait for generic metrics builders
pub trait ContainerBuilder {
    /// Initializes `BobClient` container with given name
    fn get_metrics(&self, name: &str) -> BobClient;
    /// Initializes bucket with given prefix
    fn init_bucket(&self, prefix: String) -> AtomicBucket;
}

impl<T: Output + Clone> ContainerBuilder for MetricsContainer<T> {
    fn get_metrics(&self, name: &str) -> BobClient {
        let prefix = self.prefix.clone() + ".to." + name;
        BobClient::new(&self.init_bucket(prefix))
    }

    fn init_bucket(&self, prefix: String) -> AtomicBucket {
        let bucket = AtomicBucket::new().named(prefix);
        bucket.stats(stats_all_bob);
        bucket.drain(self.output.clone());
        bucket.flush_every(self.duration);
        bucket
    }
}

fn prepare_metrics_addres(address: &str) -> String {
    address.replace(".", "_") + "."
}

/// initializes bob counters with given config and address of the local node
pub fn init_counters(
    node_config: &NodeConfig,
    local_address: &str,
) -> Arc<dyn ContainerBuilder + Send + Sync> {
    let prefix = prepare_metrics_addres(&local_address);

    let mut gr = Graphite::send_to(node_config.metrics().graphite())
        .expect("cannot init metrics for Graphite");
    gr = gr.named(node_config.name());
    let container = MetricsContainer::new(gr, Duration::from_secs(1), prefix.clone());
    info!(
        "metrics container initialized with update interval: {}ms",
        container.duration.as_millis()
    );
    let metrics = Arc::new(container);
    init_grinder(prefix.clone() + "cluster", metrics.as_ref());
    init_bob_client(prefix.clone() + "backend", metrics.as_ref());
    init_pearl(prefix + "pearl", metrics.as_ref());
    metrics
}

fn init_grinder(prefix: String, metrics: &(dyn ContainerBuilder)) {
    let bucket = metrics.init_bucket(prefix);
    GRINDER.target(bucket);
}

fn init_bob_client(prefix: String, metrics: &(dyn ContainerBuilder)) {
    let bucket = metrics.init_bucket(prefix);
    CLIENT.target(bucket);
}

#[allow(clippy::needless_pass_by_value)] // It's a callback, can't change its args
#[allow(clippy::cast_possible_truncation)] // Currently no other way to cast f64 to isize
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
