use super::prelude::*;

#[derive(PartialEq, Debug, Clone)]
pub enum Error {
    Timeout,

    VDiskNoFound(VDiskId),
    Storage(String),
    DuplicateKey,
    KeyNotFound(BobKey),
    VDiskIsNotReady,

    Failed(String),
    Internal,

    PearlChangeState(String),
}

impl Error {
    /// check if backend error causes `bob_client` reconnect
    pub fn is_service(&self) -> bool {
        match self {
            Self::Timeout | Self::Failed(_) => true,
            _ => false,
        }
    }

    /// check if put error causes pearl restart
    pub fn is_put_error_need_restart(err: Option<&Self>) -> bool {
        match err {
            Some(Self::DuplicateKey) | Some(Self::VDiskIsNotReady) | None => false,
            _ => true,
        }
    }

    /// check if put error causes put to local alien
    pub fn is_put_error_need_alien(&self) -> bool {
        match self {
            Self::DuplicateKey => false,
            _ => true,
        }
    }

    /// check if get error causes pearl restart
    pub fn is_get_error_need_restart(err: Option<&Self>) -> bool {
        match err {
            Some(Self::KeyNotFound(_)) | Some(Self::VDiskIsNotReady) | None => false,
            _ => true,
        }
    }

    /// hide backend errors
    pub fn convert_backend(self) -> Self {
        match self {
            Self::DuplicateKey | Self::KeyNotFound(_) => self,
            _ => Self::Internal,
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Self::VDiskNoFound(id) => write!(f, "vdisk: {:?} not found", id),
            Self::Storage(description) => write!(f, "backend error: {}", description),
            Self::PearlChangeState(description) => write!(f, "backend error: {}", description),
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
        trace!("Error: {}", self.clone());
        match self {
            Self::KeyNotFound(key) => Status::not_found(format!("KeyNotFound {}", key)),
            Self::DuplicateKey => Status::already_exists("DuplicateKey"),
            Self::Timeout => Status::deadline_exceeded("Timeout"),
            Self::VDiskNoFound(id) => Status::not_found(format!("VDiskNoFound {}", id)),
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
                "KeyNotFound" => parse_next(words, |n| Self::KeyNotFound(BobKey { key: n })),
                "DuplicateKey" => Some(Self::DuplicateKey),
                "Timeout" => Some(Self::Timeout),
                "VDiskNoFound" => parse_next(words, |n| Self::VDiskNoFound(VDiskId::new(n))),
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
