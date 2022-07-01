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

impl<A: Authenticator> From<CredentialsHolder<A>> for Credentials {
    fn from(holder: CredentialsHolder<A>) -> Self {
        holder.credentials
    }
}

#[derive(Debug, Clone)]
pub enum CredentialsKind {
    Basic { username: String, password: String },
    Token(String),
    InterNode(String),
}

impl CredentialsKind {
    pub fn is_internode(&self) -> bool {
        if let CredentialsKind::InterNode(_) = self {
            true
        } else {
            false
        }
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

    pub fn with_nodename(&mut self, node_name: impl Into<String>) -> &mut Self {
        self.kind = Some(CredentialsKind::InterNode(node_name.into()));
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