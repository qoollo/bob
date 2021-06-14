use std::net::SocketAddr;

#[derive(Debug)]
pub enum Credentials {
    None,
    Address(SocketAddr),
    Basic { username: String, password: String },
    Token(String),
}

impl Default for Credentials {
    fn default() -> Self {
        Credentials::None
    }
}

impl Credentials {
    pub fn basic(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self::Basic {
            username: username.into(),
            password: password.into(),
        }
    }

    pub fn token(token: impl Into<String>) -> Self {
        Self::Token(token.into())
    }

    pub fn address(address: SocketAddr) -> Self {
        Self::Address(address)
    }
}
