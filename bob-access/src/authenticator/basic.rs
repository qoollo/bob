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
    fn check_credentials(&self, credentials: Credentials) -> Result<(), Error> {
        if let Some(username) = credentials.username() {
            let user = self.users_storage.get_user(username)?;
            if let Some(password) = credentials.password() {
                if user.password() == password {
                    Ok(())
                } else {
                    Err(Error::unauthorized_request())
                }
            } else {
                Err(Error::credentials_not_provided("missing password"))
            }
        } else {
            Err(Error::credentials_not_provided("missing username"))
        }
    }
}
