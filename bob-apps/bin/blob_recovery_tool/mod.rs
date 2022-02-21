pub(crate) mod command;
pub(crate) mod utils;

pub(crate) mod prelude {
    pub(crate) use super::utils::*;
    pub(crate) use anyhow::Result as AnyResult;
    pub(crate) use bob::PearlKey;
    pub(crate) use clap::{App, Arg, ArgMatches, SubCommand};
    pub(crate) use pearl::tools::*;
    pub(crate) use pearl::Key as KeyTrait;
    pub(crate) use std::{
        io::Write,
        path::{Path, PathBuf},
        str::FromStr,
        time::{Duration, Instant},
    };
}
