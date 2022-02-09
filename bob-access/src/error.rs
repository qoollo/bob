use axum::{
    response::{IntoResponse, Response},
    Json,
};
use http::StatusCode;
use serde_json::json;
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

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        use Error::*;
        let (status, error_message) = match self {
            _Unknown => (StatusCode::INTERNAL_SERVER_ERROR, "Unknown error"),
            InvalidToken(_) => (StatusCode::BAD_REQUEST, "Invalid token"),
            Validation(_) => (StatusCode::BAD_REQUEST, "Validation error"),
            Os(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Os error"),
            UserNotFound => (StatusCode::BAD_REQUEST, "User not found"),
            ConversionError(_) => (StatusCode::BAD_REQUEST, "Conversion error"),
            CredentialsNotProvided(_) => (StatusCode::BAD_REQUEST, "Credentials not provided"),
            MultipleCredentialsTypes => (StatusCode::BAD_REQUEST, "Multiple credentials type"),
            UnauthorizedRequest => (StatusCode::UNAUTHORIZED, "Unauthorized request"),
            PermissionDenied => (StatusCode::UNAUTHORIZED, "Permission denied"),
        };
        let value = json!({
            "error": error_message,
        });
        let body = Json(value);
        (status, body).into_response()
    }
}
