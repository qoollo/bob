use crate::{credentials::Credentials, error::Error};

use self::users_storage::UsersStorageTrait;

pub mod users_storage;

pub trait Authenticator: Clone {
    fn check_credentials(&self, credentials: Credentials) -> Result<(), Error>;
}

#[derive(Debug, Default, Clone)]
pub struct StubAuthenticator<Storage: UsersStorageTrait> {
    users_storage: Storage,
}

impl<Storage: UsersStorageTrait> StubAuthenticator<Storage> {
    pub fn new(users_storage: Storage) -> Self {
        Self { users_storage }
    }
}

impl<Storage: UsersStorageTrait> Authenticator for StubAuthenticator<Storage> {
    fn check_credentials(&self, credentials: Credentials) -> Result<(), Error> {
        Ok(())
    }
}
