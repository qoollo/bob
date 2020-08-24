use super::prelude::*;

#[derive(Debug, Clone, ErrorTrait)]
pub struct Error {
    ctx: Kind,
}

impl Error {
    fn new(ctx: Kind) -> Self {
        Self { ctx }
    }
    pub(crate) fn is_not_ready(&self) -> bool {
        self.ctx == Kind::VDiskIsNotReady
    }

    pub(crate) fn is_duplicate(&self) -> bool {
        self.ctx == Kind::DuplicateKey
    }

    pub(crate) fn is_key_not_found(&self) -> bool {
        matches!(&self.ctx, Kind::KeyNotFound(_))
    }

    #[cfg(test)]
    pub(crate) fn is_internal(&self) -> bool {
        self.ctx == Kind::Internal
    }

    pub(crate) fn internal() -> Self {
        Self::new(Kind::Internal)
    }

    pub(crate) fn timeout() -> Self {
        Self::new(Kind::Timeout)
    }

    pub(crate) fn key_not_found(key: u64) -> Self {
        Self::new(Kind::KeyNotFound(key))
    }

    pub(crate) fn pearl_change_state(msg: impl Into<String>) -> Self {
        Self::new(Kind::PearlChangeState(msg.into()))
    }

    pub(crate) fn failed(cause: impl Into<String>) -> Self {
        Self::new(Kind::Failed(cause.into()))
    }

    pub(crate) fn duplicate_key() -> Self {
        Self::new(Kind::DuplicateKey)
    }

    pub(crate) fn vdisk_not_found(id: u32) -> Self {
        Self::new(Kind::VDiskNotFound(id))
    }

    pub(crate) fn vdisk_is_not_ready() -> Self {
        Self::new(Kind::VDiskIsNotReady)
    }

    pub(crate) fn storage(msg: impl Into<String>) -> Self {
        Self::new(Kind::Storage(msg.into()))
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

impl Into<Status> for Error {
    fn into(self) -> Status {
        //TODO add custom errors
        trace!("Error: {}", self);
        match &self.ctx {
            Kind::KeyNotFound(key) => Status::not_found(format!("KeyNotFound {}", key)),
            Kind::DuplicateKey => Status::already_exists("DuplicateKey"),
            Kind::Timeout => Status::deadline_exceeded("Timeout"),
            Kind::VDiskNotFound(id) => Status::not_found(format!("VDiskNotFound {}", id)),
            Kind::Storage(msg) => Status::internal(format!("Storage {}", msg)),
            Kind::VDiskIsNotReady => Status::internal("VDiskIsNotReady"),
            Kind::Failed(msg) => Status::internal(format!("Failed {}", msg)),
            Kind::Internal => Status::internal("Internal"),
            Kind::PearlChangeState(msg) => Status::internal(format!("PearlChangeState {}", msg)),
        }
    }
}

impl From<Status> for Error {
    fn from(status: Status) -> Self {
        let mut words = status.message().split_whitespace();
        let name = words.next();
        let length = status.message().len();
        match name {
            None => None,
            Some(name) => match name {
                "KeyNotFound" => parse_next(words, Self::key_not_found),
                "DuplicateKey" => Some(Self::duplicate_key()),
                "Timeout" => Some(Self::timeout()),
                "VDiskNotFound" => parse_next(words, Self::vdisk_not_found),
                "Storage" => Some(Self::storage(rest_words(words, length))),
                "VDiskIsNotReady" => Some(Self::vdisk_is_not_ready()),
                "Failed" => Some(Self::failed(rest_words(words, length))),
                "Internal" => Some(Self::internal()),
                "PearlChangeState" => Some(Self::pearl_change_state(rest_words(words, length))),
                _ => None,
            },
        }
        .unwrap_or_else(|| Self::failed("Can't parse status"))
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
    VDiskIsNotReady,
    Failed(String),
    Internal,
    PearlChangeState(String),
}
