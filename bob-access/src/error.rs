use tonic::metadata::errors::ToStrError;

#[derive(Debug)]
pub struct Error {
    kind: Kind,
}

impl Error {
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
    ConversionError(ToStrError),
    CredentialsNotProvided(String),
    MultipleCredentialsTypes,
}
