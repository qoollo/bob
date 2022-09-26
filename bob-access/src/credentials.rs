use std::net::SocketAddr;
use std::marker::PhantomData;

use axum::{
    async_trait,
    extract::{FromRequest, RequestParts},
};
use tonic::Request;

use crate::{error::Error, extractor::ExtractorExt, Authenticator};

#[derive(Debug, Default, Clone)]
pub struct Credentials {
    address: Option<Vec<SocketAddr>>,
    hostname: Option<String>,
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

    pub fn is_basic(&self) -> bool {
        if let CredentialsKind::Basic{username: _, password: _} = self {
            true
        } else {
            false
        }
    }

    pub fn is_token(&self) -> bool {
        if let CredentialsKind::Token(_) = self {
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

    pub fn ip(&self) -> &Option<Vec<SocketAddr>> {
        &self.address
    }

    pub fn set_addresses(&mut self, addresses: Vec<SocketAddr>) {
        self.address = Some(addresses);
    }

    pub fn single_ip(&self) -> Option<SocketAddr> {
        self.address.as_ref().map(|addrs| addrs[0].clone())
    }

    pub fn hostname(&self) -> &Option<String> {
        &self.hostname
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
    address: Option<Vec<SocketAddr>>,
    hostname: Option<String>,
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

    pub fn with_address(&mut self, address: Option<Vec<SocketAddr>>) -> &mut Self {
        self.address = address;
        self
    }

    pub fn with_hostname(&mut self, hostname: String) -> &mut Self {
        self.hostname = Some(hostname);
        self
    }

    pub fn with_nodename(&mut self, node_name: impl Into<String>) -> &mut Self {
        self.kind = Some(CredentialsKind::InterNode(node_name.into()));
        self
    }

    pub fn build(&self) -> Credentials {
        Credentials {
            address: self.address.clone(),
            kind: self.kind.clone(),
            hostname: self.hostname.clone(),
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