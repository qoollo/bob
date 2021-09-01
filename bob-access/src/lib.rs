#[macro_use]
extern crate log;
#[macro_use]
extern crate serde;

mod authenticator;
mod credentials;
mod error;
mod extractor;
mod settings;
mod token;

pub use authenticator::{
    basic::Basic as BasicAuthenticator, stub::Stub as StubAuthenticator, UsersMap,
};
pub use extractor::{BasicExtractor, StubExtractor};

use std::task::{Context, Poll};

use authenticator::Authenticator;
use extractor::Extractor;
use tonic::transport::NamedService;
use tower::{Layer, Service};

pub const USERS_MAP_FILE: &str = "users.yaml";

#[derive(Debug, Default)]
pub struct AccessControlLayer<A, E> {
    authenticator: Option<A>,
    extractor: Option<E>,
}

#[derive(Debug, Clone)]
pub struct AccessControlService<A, E, S> {
    authenticator: A,
    extractor: E,
    service: S,
}

impl<A, E> AccessControlLayer<A, E> {
    pub fn new() -> Self {
        Self {
            authenticator: None,
            extractor: None,
        }
    }

    pub fn with_authenticator(mut self, authenticator: A) -> Self {
        self.authenticator = Some(authenticator);
        self
    }

    pub fn with_extractor(mut self, extractor: E) -> Self {
        self.extractor = Some(extractor);
        self
    }
}

impl<S> Layer<S> for AccessControlLayer<StubAuthenticator<UsersMap>, StubExtractor> {
    type Service = AccessControlService<StubAuthenticator<UsersMap>, StubExtractor, S>;

    fn layer(&self, service: S) -> Self::Service {
        AccessControlService {
            authenticator: self.authenticator.clone().unwrap(),
            extractor: self.extractor.clone().unwrap(),
            service,
        }
    }
}

impl<S> Layer<S> for AccessControlLayer<BasicAuthenticator<UsersMap>, BasicExtractor> {
    type Service = AccessControlService<BasicAuthenticator<UsersMap>, BasicExtractor, S>;

    fn layer(&self, service: S) -> Self::Service {
        AccessControlService {
            authenticator: self.authenticator.clone().unwrap(),
            extractor: self.extractor.clone().unwrap(),
            service,
        }
    }
}

impl<A, E, S, Request> Service<Request> for AccessControlService<A, E, S>
where
    A: Authenticator,
    E: Extractor<Request>,
    S: Service<Request>,
{
    type Response = S::Response;

    type Error = S::Error;

    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        debug!("request received");
        let credentials = self.extractor.extract(&req).unwrap();
        debug!("credentials: {:#?}", credentials);
        if let Err(e) = self.authenticator.check_credentials(credentials) {
            warn!("Unauthorized request: {:?}", e);
            todo!("print log and drop request");
        } else {
            self.service.call(req)
        }
    }
}

impl<A, E, S> NamedService for AccessControlService<A, E, S>
where
    S: NamedService,
{
    const NAME: &'static str = S::NAME;
}
