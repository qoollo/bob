#[macro_use]
extern crate log;
#[macro_use]
extern crate serde;

mod authenticator;
mod credentials;
mod error;
mod extractor;
mod permissions;
mod settings;
mod token;

pub use authenticator::{
    basic::Basic as BasicAuthenticator, stub::Stub as StubAuthenticator, Authenticator, UsersMap,
};
pub use credentials::Credentials;
pub use extractor::{BasicExtractor, Extractor, StubExtractor};

use futures::{Future, FutureExt};
use http::StatusCode;
use std::{
    error::Error as StdError,
    fmt::Debug,
    pin::Pin,
    task::{Context, Poll},
};

use tonic::transport::NamedService;
use tower::{BoxError, Layer, Service};

use crate::{error::Error, permissions::GetRequiredPermissions};

pub const USERS_MAP_FILE: &str = "users.yaml";

#[derive(Debug, Default, Clone)]
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

    #[must_use]
    pub fn with_authenticator(mut self, authenticator: A) -> Self {
        self.authenticator = Some(authenticator);
        self
    }

    #[must_use]
    pub fn with_extractor(mut self, extractor: E) -> Self {
        self.extractor = Some(extractor);
        self
    }
}

impl<S, A, E> Layer<S> for AccessControlLayer<A, E>
where
    A: Clone,
    E: Clone,
{
    type Service = AccessControlService<A, E, S>;

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
    S::Error: Into<Box<dyn StdError + Send + Sync>> + 'static + Debug,
    S::Response: Send + 'static,
    S::Future: Send + 'static,
    Request: GetRequiredPermissions,
{
    type Response = S::Response;

    type Error = Box<dyn StdError + Send + Sync>;

    type Future = ServiceFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, req: Request) -> Self::Future {
        debug!("request received");
        match self.extractor.extract(&req) {
            Ok(credentials) => {
                debug!("credentials: {:#?}", credentials);
                match self.authenticator.check_credentials(credentials) {
                    Ok(permissions) => {
                        let required_permissions = req.get_permissions();
                        debug!("request requires: {}", required_permissions);
                        if permissions.contains(required_permissions) {
                            debug!("permissions granted");
                            Box::pin(self.service.call(req).map(|r| Ok(r.unwrap())))
                        } else {
                            debug!("permissions denied");
                            let error = Box::new(Error::PermissionDenied) as Self::Error;
                            Box::pin(futures::future::ready(Err(error))) as Self::Future
                        }
                    }
                    Err(e) => {
                        warn!("Unauthorized request: {:?}", e);
                        let error = Box::new(Error::UnauthorizedRequest) as Self::Error;
                        Box::pin(futures::future::ready(Err(error))) as Self::Future
                    }
                }
            }
            Err(e) => {
                let error = Box::new(e) as Self::Error;
                Box::pin(futures::future::ready(Err(error))) as Self::Future
            }
        }
    }
}

impl<A, E, S> NamedService for AccessControlService<A, E, S>
where
    S: NamedService,
{
    const NAME: &'static str = S::NAME;
}

pub async fn handle_auth_error(err: BoxError) -> (StatusCode, String) {
    error!("{}", err);
    if let Ok(err) = err.downcast::<Error>() {
        match err.as_ref() {
            Error::InvalidToken(_) => (StatusCode::FORBIDDEN, "Invalid token.".into()),
            Error::UserNotFound => (StatusCode::FORBIDDEN, "user not found".into()),
            Error::ConversionError(_) => (
                StatusCode::FORBIDDEN,
                "failed to extract credentials from request".into(),
            ),
            Error::CredentialsNotProvided(_) => {
                (StatusCode::FORBIDDEN, "credentials not provided".into())
            }
            Error::MultipleCredentialsTypes => {
                (StatusCode::FORBIDDEN, "multiple credentials types".into())
            }
            Error::UnauthorizedRequest => (
                StatusCode::FORBIDDEN,
                "unknown credentials/unauthorized request".into(),
            ),
            Error::PermissionDenied => (StatusCode::FORBIDDEN, "permission denied".into()),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "something went wrong during authorization process".into(),
            ),
        }
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, "unknown error".into())
    }
}
