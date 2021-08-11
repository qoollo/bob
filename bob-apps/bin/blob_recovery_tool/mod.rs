pub(crate) mod command;
pub(crate) mod error;
pub(crate) mod readers;
pub(crate) mod structs;
pub(crate) mod utils;

pub(crate) mod prelude {
    pub(crate) use super::error::*;
    pub(crate) use super::readers::*;
    pub(crate) use super::structs::*;
    pub(crate) use super::utils::*;
    pub(crate) use anyhow::{Context, Result as AnyResult};
    pub(crate) use bincode::serialize_into;
    pub(crate) use clap::{App, Arg, ArgMatches, SubCommand};
    pub(crate) use crc::crc32::checksum_castagnoli as crc32;
    pub(crate) use std::{
        error::Error as ErrorTrait,
        fmt::{Debug, Display, Formatter, Result as FmtResult},
        fs::{File, OpenOptions},
        io::{Read, Seek, SeekFrom, Write},
        path::{Path, PathBuf},
        str::FromStr,
        time::{Duration, Instant},
    };
}
