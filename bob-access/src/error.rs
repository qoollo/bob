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
}

#[derive(Debug)]
enum Kind {
    Unknown,
    InvalidToken(String),
}
