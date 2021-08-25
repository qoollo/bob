use crate::configs::node::{Node as NodeConfig, LOCAL_ADDRESS, METRICS_NAME, NODE_NAME};
use metrics::{register_counter, Recorder};
use metrics_exporter_prometheus::PrometheusRecorder;
use metrics_util::MetricKindMask;
use pearl::init_pearl;
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

mod exporters;
pub mod pearl;

use exporters::global_exporter::GlobalRecorder;
use exporters::graphite_exporter::GraphiteRecorder;

/// Counts number of PUT requests, processed by Grinder
pub const GRINDER_PUT_COUNTER: &str = "cluster_grinder.put_count";
/// Counts number of PUT requests return error, processed by Grinder
pub const GRINDER_PUT_ERROR_COUNT_COUNTER: &str = "cluster_grinder.put_error_count";
/// Measures processing time of the PUT request
pub const GRINDER_PUT_TIMER: &str = "cluster_grinder.put_timer";

/// Counts number of GET requests, processed by Grinder
pub const GRINDER_GET_COUNTER: &str = "cluster_grinder.get_count";
/// Counts number of GET requests return error, processed by Grinder
pub const GRINDER_GET_ERROR_COUNT_COUNTER: &str = "cluster_grinder.get_error_count";
/// Measures processing time of the GET request
pub const GRINDER_GET_TIMER: &str = "cluster_grinder.get_timer";

/// Counts number of EXIST requests, processed by Grinder
pub const GRINDER_EXIST_COUNTER: &str = "cluster_grinder.exist_count";
/// Counts number of EXIST requests return error, processed by Grinder
pub const GRINDER_EXIST_ERROR_COUNT_COUNTER: &str = "cluster_grinder.exist_error_count";
/// Measures processing time of the EXIST request
pub const GRINDER_EXIST_TIMER: &str = "cluster_grinder.exist_timer";

/// Counts number of PUT requests, processed by Client
pub const CLIENT_PUT_COUNTER: &str = "client.put_count";
/// Counts number of PUT requests return error, processed by Client
pub const CLIENT_PUT_ERROR_COUNT_COUNTER: &str = "client.put_error_count";
/// Measures processing time of the PUT request
pub const CLIENT_PUT_TIMER: &str = "client.put_timer";

/// Counts number of GET requests, processed by Client
pub const CLIENT_GET_COUNTER: &str = "client.get_count";
/// Counts number of GET requests return error, processed by Client
pub const CLIENT_GET_ERROR_COUNT_COUNTER: &str = "client.get_error_count";
/// Measures processing time of the GET request
pub const CLIENT_GET_TIMER: &str = "client.get_timer";

/// Counts number of EXIST requests, processed by Client
pub const CLIENT_EXIST_COUNTER: &str = "client.exist_count";
/// Counts number of EXIST requests return error, processed by Client
pub const CLIENT_EXIST_ERROR_COUNT_COUNTER: &str = "client.exist_error_count";
/// Measures processing time of the EXIST request
pub const CLIENT_EXIST_TIMER: &str = "client.exist_timer";

/// Observes number of connected nodes
pub const AVAILABLE_NODES_COUNT: &str = "link_manager.nodes_number";

/// Observes if bob has started already
pub const BACKEND_STATE: &str = "backend.backend_state";
/// Count blobs (without aliens)
pub const BLOBS_COUNT: &str = "backend.blob_count";
/// Count alien blobs
pub const ALIEN_BLOBS_COUNT: &str = "backend.alien_count";
/// Count memory occupied by indices
pub const INDEX_MEMORY: &str = "backend.index_memory";
/// Count disks, that are available
pub const ACTIVE_DISKS_COUNT: &str = "backend.active_disks";
/// Directory, which contains each disks state
pub const DISKS_FOLDER: &str = "backend.disks";

pub const AMOUNT_DESCRIPTORS: &str = "descr_amount";

pub const CPU_LOAD: &str = "cpu_load";
pub const FREE_RAM: &str = "free_ram";
pub const FREE_SPACE: &str = "free_space";
pub const TOTAL_RAM: &str = "total_ram";
pub const TOTAL_SPACE: &str = "total_space";

const CLIENTS_METRICS_DIR: &str = "clients";

/// Type to measure time of requests processing
pub type Timer = Instant;

/// Structure contains put/get metrics for `BobClient`
#[derive(Debug, Clone)]
pub struct BobClient {
    prefix: String,
}

impl BobClient {
    fn new(prefix: String) -> Self {
        BobClient { prefix }
    }

    pub(crate) fn put_count(&self) {
        counter!(self.prefix.clone() + ".put_count", 1);
    }

    pub(crate) fn start_timer() -> Timer {
        Instant::now()
    }

    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn put_timer_stop(&self, timer: Timer) {
        histogram!(
            self.prefix.clone() + ".put_timer",
            timer.elapsed().as_nanos() as f64
        );
    }

    pub(crate) fn put_error_count(&self) {
        counter!(self.prefix.clone() + ".put_error_count", 1);
    }

    pub(crate) fn get_count(&self) {
        counter!(self.prefix.clone() + ".get_count", 1);
    }

    pub(crate) fn get_timer_stop(&self, timer: Timer) {
        histogram!(
            self.prefix.clone() + ".get_timer",
            timer.elapsed().as_nanos() as f64
        );
    }

    pub(crate) fn get_error_count(&self) {
        counter!(self.prefix.clone() + ".get_error_count", 1);
    }

    pub(crate) fn exist_count(&self) {
        counter!(self.prefix.clone() + ".exist_count", 1);
    }

    pub(crate) fn exist_error_count(&self) {
        counter!(self.prefix.clone() + ".exist_error_count", 1);
    }

    pub(crate) fn exist_timer_stop(&self, timer: Timer) {
        histogram!(
            self.prefix.clone() + ".exist_timer",
            timer.elapsed().as_nanos() as f64
        );
    }
}

#[derive(Debug, Clone)]
struct MetricsContainer {
    duration: Duration,
    prefix: String,
}

impl MetricsContainer {
    pub(crate) fn new(duration: Duration, prefix: String) -> Self {
        MetricsContainer { duration, prefix }
    }
}

/// A trait for generic metrics builders
pub trait ContainerBuilder {
    /// Initializes `BobClient` container with given name
    fn get_metrics(&self, name: &str) -> BobClient;
}

impl ContainerBuilder for MetricsContainer {
    fn get_metrics(&self, name: &str) -> BobClient {
        let prefix = self.prefix.clone() + "." + name;
        BobClient::new(prefix)
    }
}

/// initializes bob counters with given config and address of the local node
#[allow(unused_variables)]
pub fn init_counters(
    node_config: &NodeConfig,
    local_address: &str,
) -> Arc<dyn ContainerBuilder + Send + Sync> {
    //install_prometheus();
    //install_graphite(node_config, local_address);
    install_global(node_config, local_address);
    let container = MetricsContainer::new(Duration::from_secs(1), CLIENTS_METRICS_DIR.to_owned());
    info!(
        "metrics container initialized with update interval: {}ms",
        container.duration.as_millis()
    );
    let metrics = Arc::new(container);
    init_grinder();
    init_backend();
    init_link_manager();
    init_pearl();
    metrics
}

fn init_grinder() {
    register_counter!(GRINDER_GET_COUNTER);
    register_counter!(GRINDER_PUT_COUNTER);
    register_counter!(GRINDER_EXIST_COUNTER);
    register_counter!(GRINDER_GET_ERROR_COUNT_COUNTER);
    register_counter!(GRINDER_PUT_ERROR_COUNT_COUNTER);
    register_counter!(GRINDER_EXIST_ERROR_COUNT_COUNTER);
}

fn init_backend() {
    register_counter!(BACKEND_STATE);
    register_counter!(BLOBS_COUNT);
    register_counter!(ALIEN_BLOBS_COUNT);
}

fn init_link_manager() {
    register_counter!(AVAILABLE_NODES_COUNT);
}

fn install_global(node_config: &NodeConfig, local_address: &str) {
    let graphite_rec = build_graphite(node_config, local_address);
    let prometheus_rec = build_prometheus();
    let recorders: Vec<Box<dyn Recorder>> = vec![Box::new(graphite_rec), Box::new(prometheus_rec)];
    install_global_recorder(recorders);
}

fn install_global_recorder(recorders: Vec<Box<dyn Recorder>>) {
    let global_rec = GlobalRecorder::new(recorders);
    metrics::set_boxed_recorder(Box::new(global_rec)).expect("Can't set global recorder");
}

#[allow(unused)]
fn install_prometheus() {
    let recorder = build_prometheus();
    metrics::set_boxed_recorder(Box::new(recorder)).expect("Can't set Prometheus recorder");
}

fn build_prometheus() -> PrometheusRecorder {
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .listen_address(
            "0.0.0.0:9000"
                .parse::<SocketAddr>()
                .expect("Bad metrics address"),
        )
        .idle_timeout(MetricKindMask::ALL, Some(Duration::from_secs(2)))
        .build()
}

#[allow(unused)]
fn resolve_prefix_pattern(
    mut pattern: String,
    node_config: &NodeConfig,
    local_address: &str,
) -> String {
    let mut pats_subs = vec![
        (LOCAL_ADDRESS, local_address),
        (NODE_NAME, node_config.name()),
    ];
    if let Some(name) = node_config.metrics().name() {
        pats_subs.push((METRICS_NAME, name));
    }
    for (pat, sub) in pats_subs {
        let sub = sub.to_owned().replace(|c| c == ' ' || c == '.', "_");
        pattern = pattern.replace(pat, &sub);
    }
    pattern
}

#[allow(unused)]
fn install_graphite(node_config: &NodeConfig, local_address: &str) {
    let recorder = build_graphite(node_config, local_address);
    metrics::set_boxed_recorder(Box::new(recorder)).expect("Can't set graphite recorder");
}

fn build_graphite(node_config: &NodeConfig, local_address: &str) -> GraphiteRecorder {
    let prefix_pattern = node_config
        .metrics()
        .prefix()
        .map_or(format!("{}.{}", NODE_NAME, LOCAL_ADDRESS), str::to_owned);
    let prefix = resolve_prefix_pattern(prefix_pattern, node_config, local_address);
    exporters::graphite_exporter::GraphiteBuilder::new()
        .set_address(node_config.metrics().graphite().to_string())
        .set_interval(Duration::from_secs(1))
        .set_prefix(prefix)
        .build()
}
