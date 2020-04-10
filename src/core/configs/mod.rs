/// Cluster structure configuration.
pub mod cluster;
mod cluster_tests;
/// Node confifuration.
pub mod node;
mod reader;

pub(crate) use self::cluster::{Config as ClusterConfig, Node};
pub(crate) use self::node::{BackendType, NodeConfig, PearlConfig};
use super::prelude::*;

mod prelude {
    pub(crate) use super::*;

    pub(crate) use humantime::Duration as HumanDuration;
    pub(crate) use reader::{Validatable, YamlBobConfigReader};
    pub(crate) use serde::Deserialize;
}
