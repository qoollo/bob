use crate::{credentials::Credentials, error::Error};

pub trait Authenticator {
    fn check_credentials(&self, credentials: Credentials) -> Result<(), Error>;
}
