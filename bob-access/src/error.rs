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

    pub fn unknown(message: impl Into<String>) -> Self {
        Self {
            kind: Kind::Unknown(message.into()),
        }
    }
}

#[derive(Debug)]
enum Kind {
    Unknown(String),
    CredentialsNotProvided(String),
}
