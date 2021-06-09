use crate::error::Error;

pub trait Authenticator {
    fn check_credentials(&self, credentials: Credentials) -> Result<(), Error>;
}

pub struct Credentials {}
