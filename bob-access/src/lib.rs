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
pub use credentials::Credentials;
pub use extractor::{BasicExtractor, StubExtractor};

use futures::{Future, TryFutureExt};
use std::{
    error::Error as StdError,
    pin::Pin,
    task::{Context, Poll},
};

use authenticator::Authenticator;
use error::Error;
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

type ServiceFuture<R, E> = Pin<Box<dyn Future<Output = Result<R, E>> + Send>>;

impl<A, E, S, Request> Service<Request> for AccessControlService<A, E, S>
where
    A: Authenticator,
    E: Extractor<Request>,
    S: Service<Request>,
    S::Error: Into<Box<dyn StdError + Send + Sync>> + 'static,
    S::Response: Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;

    type Error = Box<dyn StdError + Send + Sync>;

    type Future = ServiceFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        debug!("request received");
        let credentials = self.extractor.extract(&req).unwrap();
        debug!("credentials: {:#?}", credentials);
        if let Err(e) = self.authenticator.check_credentials(credentials) {
            warn!("Unauthorized request: {:?}", e);
            let error = Box::new(Error::unauthorized_request()) as Self::Error;
            Box::pin(futures::future::ready(Err(error))) as Self::Future
        } else {
            Box::pin(self.service.call(req).map_err(Into::into))
        }
    }
}

impl<A, E, S> NamedService for AccessControlService<A, E, S>
where
    S: NamedService,
{
    const NAME: &'static str = S::NAME;
}