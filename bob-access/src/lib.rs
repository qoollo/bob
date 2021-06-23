#[macro_use]
extern crate log;

mod authenticator;
mod credentials;
mod error;
mod extractor;
mod settings;

pub use authenticator::{users_storage::hashmap::UsersMap, StubAuthenticator};
pub use extractor::StubExtractor;

use std::task::{Context, Poll};

use authenticator::Authenticator;
use extractor::Extractor;
use tonic::transport::NamedService;
use tower::{Layer, Service};

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

impl<A, E, S> Layer<S> for AccessControlLayer<A, E> {
    type Service = AccessControlService<A, E, S>;

    fn layer(&self, service: S) -> Self::Service {
        let authenticator = todo!("init authenticator");
        let extractor = todo!("init extractor");
        AccessControlService {
            authenticator,
            extractor,
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
        warn!("TODO: check credentials/give access rights");
        let credentials = self.extractor.extract(&req).unwrap();
        if let Err(e) = self.authenticator.check_credentials(credentials) {
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
