use std::{error::Error as StdError, fmt::Display};
use tonic::codegen::http::header::ToStrError;

#[derive(Debug)]
pub struct Error {
    _kind: Kind,
}

#[derive(Debug)]
enum Kind {
    _Unknown,
    InvalidToken(String),
    Validation(String),
    Os(String),
    NotFound,
    ConversionError(ToStrError),
    CredentialsNotProvided(String),
    MultipleCredentialsTypes,
    UnauthorizedRequest,
}

impl Error {
    pub fn os(message: impl Into<String>) -> Self {
        Self {
            _kind: Kind::Os(message.into()),
        }
    }

    pub fn validation(message: impl Into<String>) -> Self {
        Self {
            _kind: Kind::Validation(message.into()),
        }
    }

    pub fn not_found() -> Self {
        Self {
            _kind: Kind::NotFound,
        }
    }

    pub fn invalid_token(message: impl Into<String>) -> Self {
        Self {
            _kind: Kind::InvalidToken(message.into()),
        }
    }

    pub fn credentials_not_provided(message: impl Into<String>) -> Self {
        Self {
            _kind: Kind::CredentialsNotProvided(message.into()),
        }
    }

    pub fn conversion_error(error: ToStrError) -> Self {
        Self {
            _kind: Kind::ConversionError(error),
        }
    }

    pub fn multiple_credentials_types() -> Self {
        Self {
            _kind: Kind::MultipleCredentialsTypes,
        }
    }

    pub fn unauthorized_request() -> Self {
        Self {
            _kind: Kind::UnauthorizedRequest,
        }
    }
}

impl StdError for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self)
    }
}
