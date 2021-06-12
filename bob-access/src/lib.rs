#[macro_use]
extern crate log;

mod authenticator;
mod credentials;
mod error;
mod settings;

pub use authenticator::StubAuthenticator;

use std::task::{Context, Poll};

use authenticator::Authenticator;
use tonic::transport::NamedService;
use tower::{Layer, Service};

#[derive(Debug, Default)]
pub struct AccessControlLayer<A> {
    authenticator: Option<A>,
}

#[derive(Debug, Clone)]
pub struct AccessControlService<A, S> {
    authenticator: A,
    service: S,
}

impl<A> AccessControlLayer<A> {
    pub fn new() -> Self {
        Self {
            authenticator: None,
        }
    }

    pub fn with_authenticator(mut self, authenticator: A) -> Self {
        self.authenticator = Some(authenticator);
        self
    }
}

impl<A, S> Layer<S> for AccessControlLayer<A> {
    type Service = AccessControlService<A, S>;

    fn layer(&self, service: S) -> Self::Service {
        let authenticator = todo!("init authenticator");
        AccessControlService {
            service,
            authenticator,
        }
    }
}

impl<A, S, Request> Service<Request> for AccessControlService<A, S>
where
    S: Service<Request>,
    A: Authenticator,
{
    type Response = S::Response;

    type Error = S::Error;

    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        warn!("TODO: check credentials/give access rights");
        let credentials = todo!("extract credentials from request");
        if let Err(e) = self.authenticator.check_credentials(credentials) {
            todo!("print log and drop request");
        } else {
            self.service.call(req)
        }
    }
}

impl<A, S> NamedService for AccessControlService<A, S>
where
    S: NamedService,
{
    const NAME: &'static str = S::NAME;
}
