use crate::{credentials::RequestCredentials, error::Error, permissions::Permissions};

use super::{Authenticator, AuthenticationType};

#[derive(Debug, Default, Clone)]
pub struct Stub {}

impl Stub {
    pub fn new() -> Self {
        Self {}
    }
}

impl Authenticator for Stub {
    fn check_credentials_grpc(&self, _: RequestCredentials) -> Result<Permissions, Error> {
        Ok(Permissions::all())
    }

    fn check_credentials_rest(&self, _: RequestCredentials) -> Result<Permissions, Error> {
        Ok(Permissions::all())
    }

    fn credentials_type() -> AuthenticationType {
        AuthenticationType::None
    }
}
