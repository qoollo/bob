use std::net::SocketAddr;
use std::marker::PhantomData;
use std::time::{Instant, Duration};

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

const RESOLVE_THRESHOLD: u32 = 3;

#[derive(Debug, Clone)]
enum ResolveState {
    Resolved(u32),
    InProgress,
}

impl ResolveState {
    fn update(&mut self, authenticated: bool) -> bool {
        match self {
            ResolveState::Resolved(n) => {
                if authenticated {
                    *n = 0;
                    false
                } else {
                    *n = n.wrapping_add(1);
                    *n >= RESOLVE_THRESHOLD
                }                
            },
            ResolveState::InProgress => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DeclaredCredentials {
    address: Vec<SocketAddr>,
    hostname: Option<String>,
    kind: CredentialsKind,
}

impl DeclaredCredentials {
    pub fn internode_builder(node_name: impl Into<String>) -> DeclaredCredentialsBuilder {
        DeclaredCredentialsBuilder::new(CredentialsKind::InterNode(node_name.into()))
    }

    pub fn token_builder(token: impl Into<String>) -> DeclaredCredentialsBuilder {
        DeclaredCredentialsBuilder::new(CredentialsKind::Token(token.into()))
    }

    pub fn userpass_builder(username: impl Into<String>, password: impl Into<String>,) -> DeclaredCredentialsBuilder {
        DeclaredCredentialsBuilder::new(CredentialsKind::Basic {
            username: username.into(),
            password: password.into(),
        })
    }

    pub fn ip(&self) -> &Vec<SocketAddr> {
        &self.address
    }

    pub fn replace_addresses(&mut self, addresses: Vec<SocketAddr>) {
        self.address = addresses;
    }

    pub fn hostname(&self) -> &Option<String> {
        &self.hostname
    }

    pub fn validate_internode(&self) -> bool {
        (!self.address.is_empty() || self.hostname.is_some()) &&
        self.kind.is_internode()
    }

    pub fn kind(&self) -> &CredentialsKind {
        &self.kind
    }
}

#[derive(Debug)]
pub struct DeclaredCredentialsBuilder {
    kind: CredentialsKind,
    address: Vec<SocketAddr>,
    hostname: Option<String>,
}

impl DeclaredCredentialsBuilder {
    fn new(kind: CredentialsKind) -> Self {
        Self {
            kind,
            address: Vec::new(),
            hostname: None,
        }
    }

    pub fn with_address(mut self, address: SocketAddr) -> Self {
        self.address = vec![address];
        self
    }

    pub fn with_hostname(mut self, hostname: String) -> Self {
        self.hostname = Some(hostname);
        self
    }

    pub fn build(self) -> DeclaredCredentials {
        DeclaredCredentials {
            address: self.address,
            kind: self.kind,
            hostname: self.hostname,
        }
    }
}


#[derive(Debug)]
pub struct DCredentialsResolveGuard {
    credentials: DeclaredCredentials,
    resolve_state: ResolveState,
    resolve_threshold: Instant,
    resolve_protection_period_ms: u64,
}

impl DCredentialsResolveGuard {
    pub fn new(credentials: DeclaredCredentials, resolve_protection_period_ms: u64) -> Self {
        Self {
            credentials,
            resolve_state: ResolveState::Resolved(0),
            resolve_threshold: Instant::now() + Duration::from_millis(resolve_protection_period_ms),
            resolve_protection_period_ms,
        }
    }

    pub fn update_resolve_state(&mut self, authenticated: bool) -> bool {
        if self.credentials.hostname().is_none() {
            return false;
        }
        
        if authenticated {
            self.resolve_state.update(authenticated)
        } else {
            if Instant::now() >= self.resolve_threshold {
                self.resolve_state.update(authenticated)
            } else {
                false
            }
        }
    }

    pub fn set_resolved(&mut self, addresses: Vec<SocketAddr>) {
        self.credentials.replace_addresses(addresses);
        self.resolve_state = ResolveState::Resolved(0);
        self.resolve_threshold = Instant::now() + Duration::from_millis(self.resolve_protection_period_ms);
    }

    pub fn set_in_progress(&mut self) {
        self.resolve_state = ResolveState::InProgress;
    }

    pub fn creds(&self) -> &DeclaredCredentials {
        &self.credentials
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