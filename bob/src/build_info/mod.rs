use bob_common::data::BOB_KEY_SIZE;
use pearl::build_info::BuildInfo as PearlBuildInfo;
use std::fmt::{Display, Formatter, Result as FmtResult};

use crate::build_info::build_time::BUILD_TIME;

mod build_time;

#[derive(Debug, Clone)]
pub struct BuildInfo {
    name: &'static str,
    version: &'static str,
    key_size: usize,
    commit: &'static str,
    build_time: &'static str,
    pearl: PearlBuildInfo,
}

impl BuildInfo {
    pub fn new() -> Self {
        Self {
            name: env!("CARGO_PKG_NAME"),
            version: env!("CARGO_PKG_VERSION"),
            key_size: BOB_KEY_SIZE,
            commit: option_env!("BOB_COMMIT_HASH").unwrap_or("hash-undefined"),
            build_time: BUILD_TIME,
            pearl: PearlBuildInfo::new(),
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn version(&self) -> &'static str {
        self.version
    }

    pub fn commit(&self) -> &'static str {
        self.commit
    }

    pub fn build_time(&self) -> &'static str {
        self.build_time
    }

    pub fn pearl_version(&self) -> &'static str {
        self.pearl.version()
    }

    pub fn pearl_build_time(&self) -> &'static str {
        self.pearl.build_time()
    }
}

impl Display for BuildInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        writeln!(
            f,
            "{} {} (key size: {}, commit: {}, built on: {})",
            self.name, self.version, self.key_size, self.commit, self.build_time
        )?;
        write!(f, "{}", self.pearl)
    }
}

#[test]
fn print_build_info() {
    println!("{}", BuildInfo::new());
}
