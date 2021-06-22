#[derive(Debug)]
pub struct Error {
    kind: Kind,
}

#[derive(Debug)]
enum Kind {
    Unknown,
    Validation(String),
    NotFound,
}

impl Error {
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
