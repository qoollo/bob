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
/// Counts number of keys in error EXIST requests, processed by Grinder
pub const GRINDER_EXIST_ERROR_KEYS_COUNT_COUNTER: &str = "cluster_grinder.exist_error_keys_count";
/// Counts number of keys in EXIST requests, processed by Grinder
pub const GRINDER_EXIST_KEYS_COUNT_COUNTER: &str = "cluster_grinder.exist_keys_count";
/// Measures processing time of the EXIST request
pub const GRINDER_EXIST_TIMER: &str = "cluster_grinder.exist_timer";

/// Counts number of DELETE requests, processed by Grinder
pub const GRINDER_DELETE_COUNTER: &str = "cluster_grinder.delete_count";
/// Counts number of DELETE requests return error, processed by Grinder
pub const GRINDER_DELETE_ERROR_COUNT_COUNTER: &str = "cluster_grinder.delete_error_count";
/// Measures processing time of the DELETE request
pub const GRINDER_DELETE_TIMER: &str = "cluster_grinder.delete_timer";

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
/// Counts number of keys in error EXIST requests, processed by Client
pub const CLIENT_EXIST_ERROR_KEYS_COUNT_COUNTER: &str = "client.exist_error_keys_count";
/// Counts number of keys in EXIST requests, processed by Client
pub const CLIENT_EXIST_KEYS_COUNT_COUNTER: &str = "client.exist_keys_count";
/// Measures processing time of the EXIST request
pub const CLIENT_EXIST_TIMER: &str = "client.exist_timer";

/// Counts number of DELETE requests, processed by Client
pub const CLIENT_DELETE_COUNTER: &str = "client.delete_count";
/// Counts number of DELETE requests return error, processed by Client
pub const CLIENT_DELETE_ERROR_COUNT_COUNTER: &str = "client.delete_error_count";
/// Measures processing time of the DELETE request
pub const CLIENT_DELETE_TIMER: &str = "client.delete_timer";

/// Observes number of connected nodes
pub const AVAILABLE_NODES_COUNT: &str = "link_manager.nodes_number";

/// Observes if bob has started already
pub const BACKEND_STATE: &str = "backend.backend_state";
/// Count blobs (without aliens)
pub const BLOBS_COUNT: &str = "backend.blob_count";
/// Count corrupted blobs
pub const CORRUPTED_BLOBS_COUNT: &str = "backend.corrupted_blob_count";
/// Memory occupied bybloom filters
pub const BLOOM_FILTERS_RAM: &str = "backend.bloom_filters_ram";
/// Count alien blobs
pub const ALIEN_BLOBS_COUNT: &str = "backend.alien_count";
/// Count memory occupied by indices
pub const INDEX_MEMORY: &str = "backend.index_memory";
/// Count disks, that are available
pub const ACTIVE_DISKS_COUNT: &str = "backend.active_disks";
/// Directory, which contains each disks state
pub const DISKS_FOLDER: &str = "backend.disks";
/// Directory, which contains each disks hardware metrics
pub const HW_DISKS_FOLDER: &str = "hardware.disks";
/// Occupied disk space by disks
pub const DISK_USED: &str = "backend.used_disk";

pub const DESCRIPTORS_AMOUNT: &str = "hardware.descr_amount";

pub const BOB_CPU_LOAD: &str = "hardware.bob_cpu_load";
pub const CPU_IOWAIT: &str = "hardware.cpu_iowait";
pub const AVAILABLE_RAM: &str = "hardware.available_ram";
pub const USED_RAM: &str = "hardware.used_ram";
pub const TOTAL_RAM: &str = "hardware.total_ram";
pub const USED_SWAP: &str = "hardware.used_swap";
pub const BOB_RAM: &str = "hardware.bob_ram";
pub const BOB_VIRTUAL_RAM: &str = "hardware.bob_virtual_ram";
pub const FREE_SPACE: &str = "hardware.free_space";
pub const USED_SPACE: &str = "hardware.used_space";
pub const TOTAL_SPACE: &str = "hardware.total_space";

const CLIENTS_METRICS_DIR: &str = "clients";

const PROMETHEUS_RENDER_INTERVAL_SEC: u64 = 3600;

/// Type to measure time of requests processing
pub type Timer = Instant;

/// Structure contains put/get metrics for `BobClient`
#[derive(Debug, Clone)]
pub struct BobClient {
    prefixed_names: Arc<PrefixedNames>,
}

#[derive(Debug, Clone)]
struct PrefixedNames {
    put_count: String,
    put_timer: String,
    put_error_count: String,
    get_count: String,
    get_timer: String,
    get_error_count: String,
    exist_count: String,
    exist_timer: String,
    exist_error_count: String,
    delete_count: String,
    delete_timer: String,
    delete_error_count: String,
}

impl PrefixedNames {
    fn new(prefix: &str) -> Self {
        Self {
            put_count: format!("{}.put_count", prefix),
            put_timer: format!("{}.put_timer", prefix),
            put_error_count: format!("{}.put_error_count", prefix),
            get_count: format!("{}.get_count", prefix),
            get_timer: format!("{}.get_timer", prefix),
            get_error_count: format!("{}.get_error_count", prefix),
            exist_count: format!("{}.exist_count", prefix),
            exist_timer: format!("{}.exist_timer", prefix),
            exist_error_count: format!("{}.exist_error_count", prefix),
            delete_count: format!("{}.delete_count", prefix),
            delete_timer: format!("{}.delete_timer", prefix),
            delete_error_count: format!("{}.delete_error_count", prefix),
        }
    }
}

impl BobClient {
    fn new(prefixed_names: Arc<PrefixedNames>) -> Self {
        BobClient { prefixed_names }
    }

    pub(crate) fn put_count(&self) {
        counter!(self.prefixed_names.put_count.clone(), 1);
    }

    pub(crate) fn start_timer() -> Timer {
        Instant::now()
    }

    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn put_timer_stop(&self, timer: Timer) {
        histogram!(
            self.prefixed_names.put_timer.clone(),
            timer.elapsed().as_nanos() as f64
        );
    }

    pub(crate) fn put_error_count(&self) {
        counter!(self.prefixed_names.put_error_count.clone(), 1);
    }

    pub(crate) fn get_count(&self) {
        counter!(self.prefixed_names.get_count.clone(), 1);
    }

    pub(crate) fn get_timer_stop(&self, timer: Timer) {
        histogram!(
            self.prefixed_names.get_timer.clone(),
            timer.elapsed().as_nanos() as f64
        );
    }

    pub(crate) fn get_error_count(&self) {
        counter!(self.prefixed_names.get_error_count.clone(), 1);
    }

    pub(crate) fn exist_count(&self) {
        counter!(self.prefixed_names.exist_count.clone(), 1);
    }

    pub(crate) fn exist_timer_stop(&self, timer: Timer) {
        histogram!(
            self.prefixed_names.exist_timer.clone(),
            timer.elapsed().as_nanos() as f64
        );
    }

    pub(crate) fn exist_error_count(&self) {
        counter!(self.prefixed_names.exist_error_count.clone(), 1);
    }

    pub(crate) fn delete_count(&self) {
        counter!(self.prefixed_names.delete_count.clone(), 1);
    }

    pub(crate) fn delete_timer_stop(&self, timer: Timer) {
        histogram!(
            self.prefixed_names.delete_timer.clone(),
            timer.elapsed().as_nanos() as f64
        );
    }

    pub(crate) fn delete_error_count(&self) {
        counter!(self.prefixed_names.delete_error_count.clone(), 1);
    }
}

#[derive(Debug, Clone)]
struct MetricsContainer {
    duration: Duration,
    prefixed_names: Arc<PrefixedNames>,
}

impl MetricsContainer {
    pub(crate) fn new(duration: Duration, prefix: String) -> Self {
        let prefixed_names = Arc::new(PrefixedNames::new(&prefix));
        MetricsContainer {
            duration,
            prefixed_names,
        }
    }
}

/// A trait for generic metrics builders
pub trait ContainerBuilder {
    /// Initializes `BobClient` container with given name
    fn get_metrics(&self) -> BobClient;
}

impl ContainerBuilder for MetricsContainer {
    fn get_metrics(&self) -> BobClient {
        BobClient::new(self.prefixed_names.clone())
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
    let container = MetricsContainer::new(
        Duration::from_secs(1),
        format!(
            "{}.{}",
            CLIENTS_METRICS_DIR,
            local_address.replace(".", "_")
        ),
    );
    info!(
        "metrics container initialized with update interval: {}ms",
        container.duration.as_millis()
    );
    let metrics = Arc::new(container);
    init_grinder();
    init_client();
    init_backend();
    init_link_manager();
    init_pearl();
    (metrics, shared)
}

fn init_grinder() {
    register_counter!(GRINDER_GET_COUNTER);
    register_counter!(GRINDER_PUT_COUNTER);
    register_counter!(GRINDER_EXIST_COUNTER);
    register_counter!(GRINDER_EXIST_KEYS_COUNT_COUNTER);
    register_counter!(GRINDER_GET_ERROR_COUNT_COUNTER);
    register_counter!(GRINDER_PUT_ERROR_COUNT_COUNTER);
    register_counter!(GRINDER_EXIST_ERROR_COUNT_COUNTER);
    register_counter!(GRINDER_EXIST_ERROR_KEYS_COUNT_COUNTER);
}

fn init_client() {
    register_counter!(CLIENT_EXIST_COUNTER);
    register_counter!(CLIENT_EXIST_ERROR_COUNT_COUNTER);
    register_counter!(CLIENT_EXIST_ERROR_KEYS_COUNT_COUNTER);
    register_counter!(CLIENT_EXIST_KEYS_COUNT_COUNTER);

    register_counter!(CLIENT_DELETE_COUNTER);
    register_counter!(CLIENT_DELETE_ERROR_COUNT_COUNTER);

    register_counter!(CLIENT_GET_COUNTER);
    register_counter!(CLIENT_GET_ERROR_COUNT_COUNTER);

    register_counter!(CLIENT_PUT_COUNTER);
    register_counter!(CLIENT_PUT_ERROR_COUNT_COUNTER);
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

    let handle = recorder.handle();
    let future = async move {
        let mut timer = tokio::time::interval(Duration::from_secs(PROMETHEUS_RENDER_INTERVAL_SEC));
        loop {
            let _ = timer.tick().await;
            let _ = handle.render();
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
