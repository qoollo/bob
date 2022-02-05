use crate::{
    configs::node::{Node as NodeConfig, LOCAL_ADDRESS, METRICS_NAME, NODE_NAME},
    metrics::collector::establish_global_collector,
};
use metrics::{register_counter, Recorder};
use metrics_exporter_prometheus::PrometheusRecorder;
use pearl::init_pearl;
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{pin, select};

pub mod collector;
mod exporters;
pub mod pearl;

use exporters::global_exporter::GlobalRecorder;

pub use self::collector::SharedMetricsSnapshot;

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

pub const DESCRIPTORS_AMOUNT: &str = "descr_amount";

pub const CPU_LOAD: &str = "cpu_load";
pub const FREE_RAM: &str = "free_ram";
pub const USED_RAM: &str = "used_ram";
pub const TOTAL_RAM: &str = "total_ram";
pub const BOB_RAM: &str = "bob_ram";
pub const FREE_SPACE: &str = "free_space";
pub const USED_SPACE: &str = "used_space";
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
pub async fn init_counters(
    node_config: &NodeConfig,
    local_address: &str,
) -> (
    Arc<dyn ContainerBuilder + Send + Sync>,
    SharedMetricsSnapshot,
) {
    //install_prometheus();
    //install_graphite(node_config, local_address);
    let shared = install_global(node_config, local_address).await;
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
    (metrics, shared)
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
    register_gauge!(BACKEND_STATE);
    register_gauge!(BLOBS_COUNT);
    register_gauge!(ALIEN_BLOBS_COUNT);
}

fn init_link_manager() {
    register_gauge!(AVAILABLE_NODES_COUNT);
}

async fn install_global(node_config: &NodeConfig, local_address: &str) -> SharedMetricsSnapshot {
    let (recorder, metrics) = establish_global_collector(Duration::from_secs(1));
    let mut recorders: Vec<Box<dyn Recorder>> = vec![Box::new(recorder)];

    if node_config.metrics().graphite_enabled() {
        build_graphite(node_config, local_address, metrics.clone());
    }
    if node_config.metrics().prometheus_enabled() {
        let prometheus_rec = build_prometheus(node_config);
        recorders.push(Box::new(prometheus_rec));
        info!("prometheus exporter enabled");
    } else {
        info!("prometheus exporter disabled");
    }

    if !recorders.is_empty() {
        install_global_recorder(recorders);
    }
    metrics
}

fn install_global_recorder(recorders: Vec<Box<dyn Recorder>>) {
    let global_rec = GlobalRecorder::new(recorders);
    metrics::set_boxed_recorder(Box::new(global_rec)).expect("Can't set global recorder");
}

#[allow(unused)]
fn install_prometheus(node_config: &NodeConfig) {
    let recorder = build_prometheus(node_config);
    metrics::set_boxed_recorder(Box::new(recorder)).expect("Can't set Prometheus recorder");
}

fn build_prometheus(node_config: &NodeConfig) -> PrometheusRecorder {
    let addr = node_config
        .metrics()
        .prometheus_addr()
        .parse::<SocketAddr>()
        .expect("Bad prometheus address");
    let (recorder, exporter) = metrics_exporter_prometheus::PrometheusBuilder::new()
        .listen_address(addr)
        .build_with_exporter()
        .expect("Failed to set Prometheus exporter");

    debug!("prometheus built");
    let future = async move {
        pin!(exporter);
        loop {
            select! {
                _ = &mut exporter => {}
            }
        }
    };
    tokio::spawn(future);
    recorder
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
    let (recorder, metrics) = establish_global_collector(Duration::from_secs(1));
    build_graphite(node_config, local_address, metrics);
    metrics::set_boxed_recorder(Box::new(recorder)).expect("Can't set graphite recorder");
}

fn build_graphite(node_config: &NodeConfig, local_address: &str, metrics: SharedMetricsSnapshot) {
    let prefix_pattern = node_config
        .metrics()
        .prefix()
        .map_or(format!("{}.{}", NODE_NAME, LOCAL_ADDRESS), str::to_owned);
    let prefix = resolve_prefix_pattern(prefix_pattern, node_config, local_address);
    exporters::graphite_exporter::GraphiteBuilder::new()
        .set_address(
            node_config
                .metrics()
                .graphite()
                .expect("graphite is enabled but address is not set")
                .to_string(),
        )
        .set_interval(Duration::from_secs(1))
        .set_prefix(prefix)
        .build(metrics);
}
