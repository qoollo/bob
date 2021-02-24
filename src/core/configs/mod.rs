/// Cluster structure configuration.
pub mod cluster;
mod cluster_tests;
/// Node confifuration.
pub mod node;
mod reader;

pub use self::cluster::{Cluster, Node as ClusterNode, Replica, VDisk, DistributionFunc};
pub(crate) use self::node::BackendType;
pub use self::node::{
    BackendSettings, MetricsConfig, Node, Pearl, LOCAL_ADDRESS, METRICS_NAME, NODE_NAME,
};
use super::prelude::*;

mod prelude {
    pub(crate) use super::*;

    pub(crate) use humantime::Duration as HumanDuration;
    pub(crate) use reader::{Validatable, YamlBobConfig};
    pub(crate) use serde::Deserialize;
}
