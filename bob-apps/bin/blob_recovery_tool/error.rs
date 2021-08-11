use super::prelude::*;

#[derive(Debug)]
pub(crate) enum Error {
    Validation(String),
}

impl Error {
    pub(crate) fn validation_error(message: impl Into<String>) -> Self {
        Self::Validation(message.into())
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Error::Validation(message) => write!(f, "validation error: {}", message),
        }
    }
}

impl ErrorTrait for Error {}
