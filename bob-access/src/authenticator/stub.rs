use crate::{credentials::Credentials, error::Error};

use super::{users_storage::UsersStorage, Authenticator};

#[derive(Debug, Default, Clone)]
pub struct Stub<Storage: UsersStorage> {
    _users_storage: Storage,
}

impl<Storage: UsersStorage> Stub<Storage> {
    pub fn new(_users_storage: Storage) -> Self {
        Self { _users_storage }
    }
}

impl<Storage: UsersStorage> Authenticator for Stub<Storage> {
    fn check_credentials(&self, _credentials: Credentials) -> Result<(), Error> {
        Ok(())
    }
}
