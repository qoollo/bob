use std::{error::Error as StdError, fmt::Display};
use tonic::codegen::http::header::ToStrError;

#[derive(Debug)]
pub enum Error {
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

impl StdError for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self)
    }
}
