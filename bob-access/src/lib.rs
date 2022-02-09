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
pub use error::Error;
pub use extractor::Extractor;
use permissions::GetGrpcPermissions;
pub use permissions::Permissions;

use futures::{Future, FutureExt, TryFutureExt};
use http::StatusCode;
use std::{
    error::Error as StdError,
    fmt::Debug,
    pin::Pin,
    task::{Context, Poll},
};

use tonic::transport::NamedService;
use tower::{BoxError, Layer, Service};

pub const USERS_MAP_FILE: &str = "users.yaml";

#[derive(Debug, Default, Clone)]
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

    #[must_use]
    pub fn with_authenticator(mut self, authenticator: A) -> Self {
        self.authenticator = Some(authenticator);
        self
    }
}

impl<S, A> Layer<S> for AccessControlLayer<A>
where
    A: Clone,
{
    type Service = AccessControlService<A, S>;

    fn layer(&self, service: S) -> Self::Service {
        AccessControlService {
            authenticator: self.authenticator.clone().unwrap(),
            service,
        }
    }
}

type ServiceFuture<R, E> = Pin<Box<dyn Future<Output = Result<R, E>> + Send>>;

impl<A, S, Request> Service<Request> for AccessControlService<A, S>
where
    A: Authenticator,
    S: Service<Request>,
    S::Error: Into<Box<dyn StdError + Send + Sync>> + 'static + Debug,
    S::Response: Send + 'static,
    S::Future: Send + 'static,
    Request: Extractor + GetGrpcPermissions,
{
    type Response = S::Response;

    type Error = Box<dyn StdError + Send + Sync>;

    type Future = ServiceFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, req: Request) -> Self::Future {
        debug!("request received");
        let error = match req.extract() {
            Ok(credentials) => {
                debug!("credentials: {:#?}", credentials);
                match self.authenticator.check_credentials(credentials) {
                    Ok(permissions) => {
                        if let Some(required_permissions) = req.get_grpc_permissions() {
                            debug!("request requires: {}", required_permissions);
                            if permissions.contains(required_permissions) {
                                debug!("permissions granted");
                                return Box::pin(self.service.call(req).map_err(|e| e.into()));
                            } else {
                                debug!("permissions denied");
                                Error::PermissionDenied
                            }
                        } else {
                            Error::NotGrpcRequest
                        }
                    }
                    Err(e) => {
                        warn!("Wrong credentials: {:?}", e);
                        e
                    }
                }
            }
            Err(e) => e,
        };
        futures::future::err(Box::new(error) as Self::Error).boxed()
    }
}

impl<A, S> NamedService for AccessControlService<A, S>
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
