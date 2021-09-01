use crate::{credentials::Credentials, error::Error};

use super::{users_storage::UsersStorage, Authenticator};

#[derive(Debug, Default, Clone)]
pub struct Basic<Storage: UsersStorage> {
    users_storage: Storage,
}

impl<Storage: UsersStorage> Basic<Storage> {
    pub fn new(users_storage: Storage) -> Self {
        Self { users_storage }
    }
}

impl<Storage: UsersStorage> Authenticator for Basic<Storage> {
    fn check_credentials(&self, _credentials: Credentials) -> Result<(), Error> {
        todo!();
        Ok(())
    }
}
