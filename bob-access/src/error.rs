use std::{error::Error as StdError, fmt::Display};
use tonic::codegen::http::header::ToStrError;

#[derive(Debug)]
pub struct Error {
    kind: Kind,
}

#[derive(Debug)]
pub enum Kind {
    _Unknown,
    InvalidToken(String),
    Validation(String),
    Os(String),
    UserNotFound,
    ConversionError(ToStrError),
    CredentialsNotProvided(String),
    MultipleCredentialsTypes,
    UnauthorizedRequest,
    PermissionDenied,
}

impl Error {
    pub fn kind(&self) -> &Kind {
        &self.kind
    }

    pub fn os(message: impl Into<String>) -> Self {
        Self {
            kind: Kind::Os(message.into()),
        }
    }

    pub fn validation(message: impl Into<String>) -> Self {
        Self {
            kind: Kind::Validation(message.into()),
        }
    }

    pub fn user_not_found() -> Self {
        Self {
            kind: Kind::UserNotFound,
        }
    }

    pub fn invalid_token(message: impl Into<String>) -> Self {
        Self {
            kind: Kind::InvalidToken(message.into()),
        }
    }

    pub fn credentials_not_provided(message: impl Into<String>) -> Self {
        Self {
            kind: Kind::CredentialsNotProvided(message.into()),
        }
    }

    pub fn conversion_error(error: ToStrError) -> Self {
        Self {
            kind: Kind::ConversionError(error),
        }
    }

    pub fn multiple_credentials_types() -> Self {
        Self {
            kind: Kind::MultipleCredentialsTypes,
        }
    }

    pub fn unauthorized_request() -> Self {
        Self {
            kind: Kind::UnauthorizedRequest,
        }
    }

    pub fn permission_denied() -> Self {
        Self {
            kind: Kind::PermissionDenied,
        }
    }
}

impl StdError for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self)
    }
}
