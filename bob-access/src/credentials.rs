use std::net::SocketAddr;
use std::marker::PhantomData;

use axum::{
    async_trait,
    extract::{FromRequest, RequestParts},
};
use tonic::Request;

use crate::{error::Error, extractor::ExtractorExt, Authenticator};

#[derive(Debug, Default)]
pub struct RequestCredentials {
    address: Option<SocketAddr>,
    kind: Option<CredentialsKind>,
}

impl RequestCredentials {
    pub fn builder() -> RequestCredentialsBuilder {
        RequestCredentialsBuilder::default()
    }

    pub fn kind(&self) -> Option<&CredentialsKind> {
        self.kind.as_ref()
    }

    pub fn ip(&self) -> Option<SocketAddr> {
        self.address
    }
}

const RESOLVE_THRESHOLD: u8 = 3;

#[derive(Debug, Clone)]
enum ResolveState {
    Waiting(u8),
    Pending,
}

impl Default for ResolveState {
    fn default() -> Self {
        ResolveState::Waiting(0)
    }
}

impl ResolveState {
    fn needs_resolve(&mut self, unresolved: bool) -> bool {
        match self {
            ResolveState::Waiting(n) => {
                if unresolved {
                    *n += 1;
                    *n >= RESOLVE_THRESHOLD
                } else {
                    *n = 0;
                    false
                }                
            },
            ResolveState::Pending => false,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct DeclaredCredentials {
    address: Option<Vec<SocketAddr>>,
    hostname: Option<String>,
    kind: Option<CredentialsKind>,
    resolve_state: ResolveState,
}

pub struct CredentialsHolder<A: Authenticator> {
    credentials: RequestCredentials,
    pd: PhantomData<A>,
}

impl<A: Authenticator> From<CredentialsHolder<A>> for RequestCredentials {
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

impl DeclaredCredentials {
    pub fn builder() -> DeclaredCredentialsBuilder {
        DeclaredCredentialsBuilder::default()
    }

    pub fn ip(&self) -> &Option<Vec<SocketAddr>> {
        &self.address
    }

    pub fn add_addresses(&mut self, mut addresses: Vec<SocketAddr>) {
        match self.address.as_mut() {
            Some(address) => address.extend(addresses.drain(..)),
            None => self.address = Some(addresses),
        }
    }

    pub fn needs_resolve(&mut self, unresolved: bool) -> bool {
        self.resolve_state.needs_resolve(unresolved)
    }

    pub fn set_resolved(&mut self) {
        self.resolve_state = ResolveState::Waiting(0);
    }

    pub fn set_pending(&mut self) {
        self.resolve_state = ResolveState::Pending;
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

#[derive(Default)]
pub struct RequestCredentialsBuilder {
    kind: Option<CredentialsKind>,
    address: Option<SocketAddr>,
}

impl RequestCredentialsBuilder {
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

    pub fn with_nodename(mut self, node_name: impl Into<String>) -> Self {
        self.kind = Some(CredentialsKind::InterNode(node_name.into()));
        self
    }

    pub fn build(self) -> RequestCredentials {
        RequestCredentials {
            address: self.address,
            kind: self.kind,
        }
    }
}

#[derive(Debug, Default)]
pub struct DeclaredCredentialsBuilder {
    kind: Option<CredentialsKind>,
    address: Option<Vec<SocketAddr>>,
    hostname: Option<String>,
    resolve_state: ResolveState,
}

impl DeclaredCredentialsBuilder {
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

    pub fn with_address(mut self, address: Option<Vec<SocketAddr>>) -> Self {
        self.address = address;
        self.resolve_state = ResolveState::Waiting(0);
        self
    }

    pub fn with_hostname(mut self, hostname: String) -> Self {
        self.hostname = Some(hostname);
        self
    }

    pub fn with_nodename(mut self, node_name: impl Into<String>) -> Self {
        self.kind = Some(CredentialsKind::InterNode(node_name.into()));
        self
    }

    pub fn build(self) -> DeclaredCredentials {
        DeclaredCredentials {
            address: self.address,
            kind: self.kind,
            hostname: self.hostname,
            resolve_state: self.resolve_state,
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
            Err(_) => RequestCredentials::default()
        };
        CredentialsHolder {
            credentials,
            pd: PhantomData,
        }
    }
}