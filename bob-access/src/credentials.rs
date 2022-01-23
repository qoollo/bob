use std::net::{IpAddr, SocketAddr};

#[derive(Debug, Default, Clone)]
pub struct Credentials {
    address: Option<SocketAddr>,
    kind: Option<CredentialsKind>,
}

#[derive(Debug, Clone)]
pub enum CredentialsKind {
    Basic { username: String, password: String },
    Token(String),
}

impl Credentials {
    pub fn builder() -> CredentialsBuilder {
        CredentialsBuilder::default()
    }

    pub fn username(&self) -> Option<&str> {
        let kind = self.kind.as_ref()?;
        match kind {
            CredentialsKind::Basic {
                username,
                password: _,
            } => Some(username),
            CredentialsKind::Token(_) => None,
        }
    }

    pub fn password(&self) -> Option<&str> {
        let kind = self.kind.as_ref()?;
        match kind {
            CredentialsKind::Basic {
                username: _,
                password,
            } => Some(password),
            CredentialsKind::Token(_) => None,
        }
    }

    pub fn ip(&self) -> Option<IpAddr> {
        Some(self.address?.ip())
    }

    pub fn is_complete(&self) -> bool {
        self.address.is_some() && self.ip().is_some()
    }
}

#[derive(Debug, Default)]
pub struct CredentialsBuilder {
    kind: Option<CredentialsKind>,
    address: Option<SocketAddr>,
}

impl CredentialsBuilder {
    pub fn with_username_password(
        &mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> &mut Self {
        self.kind = Some(CredentialsKind::Basic {
            username: username.into(),
            password: password.into(),
        });
        self
    }

    pub fn with_token(&mut self, token: impl Into<String>) -> &mut Self {
        self.kind = Some(CredentialsKind::Token(token.into()));
        self
    }

    pub fn with_address(&mut self, address: Option<SocketAddr>) -> &mut Self {
        self.address = address;
        self
    }

    pub fn build(&self) -> Credentials {
        Credentials {
            address: self.address,
            kind: self.kind.clone(),
        }
    }
}
