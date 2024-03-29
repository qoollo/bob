use std::fmt::{Display, Formatter, Result as FmtResult};
use thiserror::Error as ErrorTrait;
use tonic::Status;

use crate::data::BobKey;
use crate::core_types::VDiskId;

#[derive(Debug, Clone, ErrorTrait)]
pub struct Error {
    ctx: Kind,
}

impl Error {
    fn new(ctx: Kind) -> Self {
        Self { ctx }
    }

    pub fn kind(&self) -> &Kind {
        &self.ctx
    }

    pub fn is_not_ready(&self) -> bool {
        self.ctx == Kind::VDiskIsNotReady
    }

    pub fn is_duplicate(&self) -> bool {
        self.ctx == Kind::DuplicateKey
    }

    pub fn is_key_not_found(&self) -> bool {
        matches!(&self.ctx, Kind::KeyNotFound(_))
    }

    pub fn is_internal(&self) -> bool {
        self.ctx == Kind::Internal
    }

    pub fn is_possible_disk_disconnection(&self) -> bool {
        self.ctx == Kind::PossibleDiskDisconnection
    }

    pub fn internal() -> Self {
        Self::new(Kind::Internal)
    }

    pub fn unauthorized() -> Self {
        Self::new(Kind::Unauthorized)
    }

    pub fn timeout() -> Self {
        Self::new(Kind::Timeout)
    }

    pub fn key_not_found(key: BobKey) -> Self {
        Self::new(Kind::KeyNotFound(key))
    }

    pub fn pearl_change_state(msg: impl Into<String>) -> Self {
        Self::new(Kind::PearlChangeState(msg.into()))
    }

    pub fn failed(cause: impl Into<String>) -> Self {
        Self::new(Kind::Failed(cause.into()))
    }

    pub fn duplicate_key() -> Self {
        Self::new(Kind::DuplicateKey)
    }

    pub fn vdisk_not_found(id: u32) -> Self {
        Self::new(Kind::VDiskNotFound(id))
    }

    pub fn dc_is_not_available() -> Self {
        Self::new(Kind::DCIsNotAvailable)
    }

    pub fn possible_disk_disconnection() -> Self {
        Self::new(Kind::PossibleDiskDisconnection)
    }

    pub fn vdisk_is_not_ready() -> Self {
        Self::new(Kind::VDiskIsNotReady)
    }

    pub fn storage(msg: impl Into<String>) -> Self {
        Self::new(Kind::Storage(msg.into()))
    }

    pub fn holder_temporary_unavailable() -> Self {
        Self::new(Kind::HolderTemporaryUnavailable)
    }

    pub fn request_failed_completely(local: &Error, alien: &Error) -> Self {
        let msg = format!("local error: {}\nalien error: {}", local, alien);
        let ctx = Kind::RequestFailedCompletely(msg);
        Self::new(ctx)
    }

    pub fn disk_events_logger(msg: impl Display, error: impl Display) -> Self {
        Self::new(Kind::DisksEventsLogger(format!("{}: {}", msg, error)))
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        Display::fmt(&self.ctx, f)
    }
}

impl Display for Kind {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::VDiskNotFound(id) => write!(f, "vdisk: {:?} not found", id),
            Self::Storage(description) => write!(f, "backend storage error: {}", description),
            Self::PearlChangeState(description) => {
                write!(f, "backend pearl change state error: {}", description)
            }
            err => write!(f, "{:?}", err),
        }
    }
}

impl From<Error> for Status {
    fn from(err: Error) -> Self {
        //TODO add custom errors
        trace!("Error: {}", err);
        match &err.ctx {
            Kind::KeyNotFound(key) => Self::not_found(format!("KeyNotFound {}", key)),
            Kind::DuplicateKey => Self::already_exists("DuplicateKey"),
            Kind::Timeout => Self::deadline_exceeded("Timeout"),
            Kind::VDiskNotFound(id) => Self::not_found(format!("VDiskNotFound {}", id)),
            Kind::Storage(msg) => Self::internal(format!("Storage {}", msg)),
            Kind::VDiskIsNotReady => Self::internal("VDiskIsNotReady"),
            Kind::DCIsNotAvailable => Status::internal("Disk Controller is not available"),
            Kind::PossibleDiskDisconnection => Self::internal("Possibly disk was disconnected"),
            Kind::Failed(msg) => Self::internal(format!("Failed {}", msg)),
            Kind::Internal => Self::internal("Internal"),
            Kind::PearlChangeState(msg) => Self::internal(format!("PearlChangeState {}", msg)),
            Kind::RequestFailedCompletely(msg) => Self::internal(format!(
                "Request failed on both stages local and alien: {}",
                msg
            )),
            Kind::DisksEventsLogger(msg) => {
                Self::internal(format!("disk events logger error: {}", msg))
            },
            Kind::Unauthorized => Self::unauthenticated("Unauthorized"),
            Kind::HolderTemporaryUnavailable => Self::unavailable("HolderTemporaryUnavailable"),
        }
    }
}

impl From<Status> for Error {
    fn from(status: Status) -> Self {
        let mut words = status.message().split_whitespace();
        let name = words.next();
        let length = status.message().len();
        match name {
            None => Self::failed(format!("Can't parse status from {:?}, {}", status.code(), status.message())),
            Some(name) => match name {
                "KeyNotFound" => parse_next(words, Self::key_not_found)
                    .unwrap_or_else(|| Self::failed(format!("Failed to parse key from {}", status.message()))),
                "DuplicateKey" => Self::duplicate_key(),
                "Timeout" => Self::timeout(),
                "VDiskNotFound" => parse_next(words, Self::vdisk_not_found)
                    .unwrap_or_else(|| Self::failed(format!("Failed to parse vdisk_id from {}", status.message()))),
                "Storage" => Self::storage(rest_words(words, length)),
                "VDiskIsNotReady" => Self::vdisk_is_not_ready(),
                "Failed" => Self::failed(rest_words(words, length)),
                "Internal" => Self::internal(),
                "PearlChangeState" => Self::pearl_change_state(rest_words(words, length)),
                "Unauthorized" => Self::unauthorized(),
                "HolderTemporaryUnavailable" => Self::holder_temporary_unavailable(),
                _ => Self::failed(format!("Can't parse status {:?} from {:?}, {}", name, status.code(), status.message())),
            },
        }
    }
}

fn rest_words<'a>(words: impl Iterator<Item = &'a str>, length: usize) -> String {
    words.fold(String::with_capacity(length), |s, n| s + n)
}

fn parse_next<'a, T, Y>(mut words: impl Iterator<Item = &'a str>, f: impl Fn(T) -> Y) -> Option<Y>
where
    T: std::str::FromStr,
{
    words.next().and_then(|w| w.parse().ok()).map(f)
}

#[derive(PartialEq, Debug, Clone)]
pub enum Kind {
    Timeout,
    VDiskNotFound(VDiskId),
    Storage(String),
    DuplicateKey,
    KeyNotFound(BobKey),
    DCIsNotAvailable,
    PossibleDiskDisconnection,
    VDiskIsNotReady,
    Failed(String),
    Internal,
    PearlChangeState(String),
    RequestFailedCompletely(String),
    DisksEventsLogger(String),
    Unauthorized,
    HolderTemporaryUnavailable,
}
