use tonic::metadata::errors::ToStrError;

#[derive(Debug)]
pub struct Error {
    kind: Kind,
}

impl Error {
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
}

impl Error {
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

    pub fn not_found() -> Self {
        Self {
            kind: Kind::NotFound,
        }
    }
}
