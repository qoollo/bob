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
            Self::KeyNotFound(key) => Status::not_found(&format!("KeyNotFound: {}", key)),
            Self::DuplicateKey => Status::already_exists("DuplicateKey"),
            _ => Status::unknown("Other errors"),
        }
    }
}

impl From<Status> for Error {
    fn from(status: Status) -> Self {
        match status.code() {
            // TODO: Find better solution for passing key
            Code::NotFound => Self::KeyNotFound(BobKey {
                key: status.message()[13..].parse().unwrap_or_default(),
            }),
            _ => Self::Failed(format!("{:?}", status)),
        }
    }
}
