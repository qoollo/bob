use std::net::SocketAddr;

#[derive(Debug, Default)]
pub struct Credentials {
    address: Option<SocketAddr>,
    kind: Option<CredentialsKind>,
}

#[derive(Debug)]
pub enum CredentialsKind {
    Basic { username: String, password: String },
    Token(String),
}

impl Credentials {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_username_password(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.kind = Some(CredentialsKind::Basic {
            username: username.into(),
            password: password.into(),
        });
        self
    }

    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.kind = Some(CredentialsKind::Token(token.into()));
        self
    }

    pub fn with_address(mut self, address: Option<SocketAddr>) -> Self {
        self.address = address;
        self
    }
}
