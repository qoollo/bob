#[macro_use]
extern crate log;

mod authenticator;
mod credentials;
mod error;
mod settings;

use std::task::{Context, Poll};

use tonic::transport::NamedService;
use tower::{Layer, Service};

#[derive(Debug, Default)]
pub struct AccessControlLayer {}

#[derive(Debug, Clone)]
pub struct AccessControlService<S> {
    service: S,
}

impl AccessControlLayer {
    pub fn new() -> Self {
        Self {}
    }
}

impl<S> Layer<S> for AccessControlLayer {
    type Service = AccessControlService<S>;

    fn layer(&self, service: S) -> Self::Service {
        AccessControlService { service }
    }
}

impl<S, Request> Service<Request> for AccessControlService<S>
where
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
        self.service.call(req)
    }
}

impl<S> NamedService for AccessControlService<S>
where
    S: NamedService,
{
    const NAME: &'static str = S::NAME;
}
