use std::net::{IpAddr, SocketAddr};
use std::marker::PhantomData;

use axum::{
    async_trait,
    extract::{FromRequest, RequestParts},
};
use tonic::Request;

use crate::{error::Error, extractor::ExtractorExt, Authenticator};

#[derive(Debug, Default, Clone)]
pub struct Credentials {
    address: Option<SocketAddr>,
    kind: Option<CredentialsKind>,
}

pub struct CredentialsHolder<A: Authenticator> {
    credentials: Credentials,
    pd: PhantomData<A>,
}

impl<A: Authenticator> CredentialsHolder<A> {
    pub fn into_credentials(self) -> Credentials {
        self.credentials
    }
}

#[derive(Debug, Clone)]
pub enum CredentialsKind {
    Basic { username: String, password: String },
    Token(String),
}

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
pub enum CredentialsType {
    None,
    Basic,
    Token,
}

impl CredentialsType {
    pub fn is_basic(&self) -> bool {
        *self == CredentialsType::Basic
    }

    pub fn is_stub(&self) -> bool {
        *self == CredentialsType::None
    }
}

impl Credentials {
    pub fn builder() -> CredentialsBuilder {
        CredentialsBuilder::default()
    }

    pub fn ip(&self) -> Option<IpAddr> {
        Some(self.address?.ip())
    }

    pub fn is_complete(&self) -> bool {
        self.address.is_some() && self.ip().is_some()
    }

    pub fn kind(&self) -> Option<&CredentialsKind> {
        self.kind.as_ref()
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

#[async_trait]
impl<B, A: Authenticator> FromRequest<B> for CredentialsHolder<A>
where
    B: Send,
{
    type Rejection = Error;

    async fn from_request(req: &mut RequestParts<B>) -> Result<Self, Self::Rejection> {
        Ok(CredentialsHolder {
            credentials: req.extract(A::credentials_type())?,
            pd: PhantomData,
        })
    }
}

impl<T, A: Authenticator> From<&Request<T>> for CredentialsHolder<A> {
    fn from(req: &Request<T>) -> Self {
        let credentials = match req.extract(A::credentials_type()) {
            Ok(c) => c,
            Err(_) => Credentials::default()
        };
        CredentialsHolder {
            credentials,
            pd: PhantomData,
        }
    }
}