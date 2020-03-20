use super::prelude::*;

#[derive(PartialEq, Debug, Clone)]
pub enum Error {
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

impl Error {
    pub(crate) fn is_not_ready(&self) -> bool {
        self == &Self::VDiskIsNotReady
    }

    pub(crate) fn is_duplicate(&self) -> bool {
        self == &Self::DuplicateKey
    }

    pub(crate) fn is_key_not_found(&self) -> bool {
        if let Self::KeyNotFound(_) = self {
            true
        } else {
            false
        }
    }
}

impl Display for Error {
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

impl From<IOError> for Error {
    fn from(error: IOError) -> Self {
        match error.kind() {
            ErrorKind::TimedOut => Self::Timeout,
            _ => Self::Failed(format!("Ping operation failed: {:?}", error)),
        }
    }
}

impl Into<Status> for Error {
    fn into(self) -> Status {
        //TODO add custom errors
        trace!("Error: {}", self);
        match self {
            Self::KeyNotFound(key) => Status::not_found(format!("KeyNotFound {}", key)),
            Self::DuplicateKey => Status::already_exists("DuplicateKey"),
            Self::Timeout => Status::deadline_exceeded("Timeout"),
            Self::VDiskNotFound(id) => Status::not_found(format!("VDiskNotFound {}", id)),
            Self::Storage(msg) => Status::internal(format!("Storage {}", msg)),
            Self::VDiskIsNotReady => Status::internal("VDiskIsNotReady"),
            Self::Failed(msg) => Status::internal(format!("Failed {}", msg)),
            Self::Internal => Status::internal("Internal"),
            Self::PearlChangeState(msg) => Status::internal(format!("PearlChangeState {}", msg)),
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
                "KeyNotFound" => parse_next(words, Self::KeyNotFound),
                "DuplicateKey" => Some(Self::DuplicateKey),
                "Timeout" => Some(Self::Timeout),
                "VDiskNotFound" => parse_next(words, Self::VDiskNotFound),
                "Storage" => Some(Self::Storage(rest_words(words, length))),
                "VDiskIsNotReady" => Some(Self::VDiskIsNotReady),
                "Failed" => Some(Self::Failed(rest_words(words, length))),
                "Internal" => Some(Self::Internal),
                "PearlChangeState" => Some(Self::PearlChangeState(rest_words(words, length))),
                _ => None,
            },
        }
        .unwrap_or_else(|| Self::Failed("Can't parse status".to_string()))
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
