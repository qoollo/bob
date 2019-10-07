use super::prelude::*;

#[derive(PartialEq, Debug, Clone)]
pub enum Error {
    Timeout,

    VDiskNoFound(VDiskId),
    StorageError(String),
    DuplicateKey,
    KeyNotFound,
    VDiskIsNotReady,

    Failed(String),
    Internal,
}

impl Error {
    /// check if backend error causes 'bob_client' reconnect
    pub fn is_service(&self) -> bool {
        match self {
            Error::Timeout | Error::Failed(_) => true,
            _ => false,
        }
    }

    /// check if put error causes pearl restart
    pub fn is_put_error_need_restart(err: Option<&Error>) -> bool {
        match err {
            Some(Error::DuplicateKey) | Some(Error::VDiskIsNotReady) => false,
            Some(_) => true,
            _ => false,
        }
    }

    /// check if put error causes put to local alien
    pub fn is_put_error_need_alien(&self) -> bool {
        match self {
            Error::DuplicateKey => false,
            _ => true,
        }
    }

    /// check if get error causes pearl restart
    pub fn is_get_error_need_restart(err: Option<&Error>) -> bool {
        match err {
            Some(Error::KeyNotFound) | Some(Error::VDiskIsNotReady) => false,
            Some(_) => true,
            _ => false,
        }
    }

    /// hide backend errors
    pub fn convert_backend(self) -> Error {
        match self {
            Error::DuplicateKey | Error::KeyNotFound => self,
            _ => Error::Internal,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::VDiskNoFound(id) => write!(f, "vdisk: {:?} not found", id),
            Error::StorageError(description) => write!(f, "backend error: {}", description),
            err => write!(f, "{:?}", err),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        match error.kind() {
            ErrorKind::TimedOut => Error::Timeout,
            _ => Error::Failed(format!("Ping operation failed: {:?}", error)),
        }
    }
}

impl Into<Status> for Error {
    fn into(self) -> Status {
        //TODO add custom errors
        trace!("Error: {}", self.clone());
        match self {
            Error::KeyNotFound => Status::new(Code::Unknown, format!("KeyNotFound")),
            Error::DuplicateKey => Status::new(Code::Unknown, format!("DuplicateKey")),
            _ => Status::new(Code::Unknown, format!("Other errors")),
        }
    }
}

impl From<Status> for Error {
    fn from(error: Status) -> Self {
        match error.code() {
            Code::Unknown => match error.message() {
                "KeyNotFound" => Error::KeyNotFound,
                "DuplicateKey" => Error::DuplicateKey,
                _ => Error::Internal,
            },
            _ => Error::Failed(format!("grpc error: {}", error)),
        }
    }
}

impl From<TimerError> for Error {
    fn from(error: TimerError) -> Self {
        if error.is_elapsed() {
            return Error::Timeout;
        }
        if error.is_timer() {
            return Error::Failed(format!("error in timer: {}", error));
        }
        match error.into_inner() {
            Some(status) => Error::from(status),
            _ => Error::Failed("failed grpc operation".to_string()),
        }
    }
}
