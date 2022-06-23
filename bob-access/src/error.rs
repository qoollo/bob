use axum::{
    response::{IntoResponse, Response},
    Json,
};
use http::StatusCode;
use serde_json::json;
use std::{error::Error as StdError, fmt::Display};
use tonic::{Code, Status};

#[derive(Debug)]
pub enum Error {
    _Unknown,
    InvalidToken(String),
    Validation(String),
    Os(String),
    UserNotFound,
    ConversionError(String),
    CredentialsNotProvided(String),
    MultipleCredentialsTypes,
    UnauthorizedRequest,
    PermissionDenied,
}

impl Error {
    pub fn msg(&self) -> &'static str {
        use Error::*;
        match self {
            _Unknown => "Unknown error",
            InvalidToken(_) => "Invalid token",
            Validation(_) => "Validation error",
            Os(_) => "Os error",
            UserNotFound => "User not found",
            ConversionError(_) => "Conversion error",
            CredentialsNotProvided(_) => "Credentials not provided",
            MultipleCredentialsTypes => "Multiple credentials type",
            UnauthorizedRequest => "Unauthorized request",
            PermissionDenied => "Permission denied",
        }
    }

    pub fn status_code(&self) -> StatusCode {
        use Error::*;
        match self {
            _Unknown => StatusCode::INTERNAL_SERVER_ERROR,
            InvalidToken(_) => StatusCode::BAD_REQUEST,
            Validation(_) => StatusCode::BAD_REQUEST,
            Os(_) => StatusCode::INTERNAL_SERVER_ERROR,
            UserNotFound => StatusCode::UNAUTHORIZED,
            ConversionError(_) => StatusCode::BAD_REQUEST,
            CredentialsNotProvided(_) => StatusCode::UNAUTHORIZED,
            MultipleCredentialsTypes => StatusCode::BAD_REQUEST,
            UnauthorizedRequest => StatusCode::UNAUTHORIZED,
            PermissionDenied => StatusCode::FORBIDDEN,
        }
    }

    pub fn code(&self) -> Code {
        use Error::*;
        match self {
            _Unknown => Code::Unknown,
            InvalidToken(_) => Code::InvalidArgument,
            Validation(_) => Code::InvalidArgument,
            Os(_) => Code::Internal,
            UserNotFound => Code::PermissionDenied,
            ConversionError(_) => Code::InvalidArgument,
            CredentialsNotProvided(_) => Code::InvalidArgument,
            MultipleCredentialsTypes => Code::InvalidArgument,
            UnauthorizedRequest => Code::PermissionDenied,
            PermissionDenied => Code::PermissionDenied,
        }
    }
}

impl StdError for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self)
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let value = json!({
            "error": self.msg(),
        });
        let body = Json(value);
        (self.status_code(), body).into_response()
    }
}

impl From<Error> for Status {
    fn from(err: Error) -> Self {
        let message = err.msg();
        let code = err.code();
        Status::new(code, message)
    }
}
