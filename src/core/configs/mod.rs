/// Cluster structure configuration.
pub mod cluster;
mod cluster_tests;
/// Node confifuration.
pub mod node;
mod reader;

pub(crate) use self::cluster::{Cluster, Node as ClusterNode};
pub(crate) use self::node::{BackendType, Node, Pearl};
use super::prelude::*;

mod prelude {
    pub(crate) use super::*;

    pub(crate) use humantime::Duration as HumanDuration;
    pub(crate) use reader::{Validatable, YamlBobConfig};
    pub(crate) use serde::Deserialize;
}
