mod users_storage;

use crate::{credentials::Credentials, error::Error};

use self::users_storage::UsersStorage;

pub use users_storage::UsersMap;

pub trait Authenticator: Clone {
    fn check_credentials(&self, credentials: Credentials) -> Result<(), Error>;
}

#[derive(Debug, Default, Clone)]
pub struct StubAuthenticator<Storage: UsersStorage> {
    users_storage: Storage,
}

impl<Storage: UsersStorage> StubAuthenticator<Storage> {
    pub fn new(users_storage: Storage) -> Self {
        Self { users_storage }
    }
}

impl<Storage: UsersStorage> Authenticator for StubAuthenticator<Storage> {
    fn check_credentials(&self, _credentials: Credentials) -> Result<(), Error> {
        Ok(())
    }
}
