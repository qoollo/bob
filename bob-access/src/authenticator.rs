use crate::{credentials::Credentials, error::Error};

pub trait Authenticator: Clone {
    fn check_credentials(&self, credentials: Credentials) -> Result<(), Error>;
}

#[derive(Debug, Default, Clone)]
pub struct StubAuthenticator {}

impl StubAuthenticator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Authenticator for StubAuthenticator {
    fn check_credentials(&self, credentials: Credentials) -> Result<(), Error> {
        Ok(())
    }
}
