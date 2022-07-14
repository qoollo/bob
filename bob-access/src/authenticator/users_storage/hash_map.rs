use std::collections::HashMap;

use crate::error::Error;

use super::{
    config::{parse_users, ConfigUsers},
    User, UsersStorage,
};

#[derive(Default, Clone, Debug)]
pub struct UsersMap {
    inner: HashMap<String, User>,
}

impl UsersMap {
    pub fn from_config(config: ConfigUsers) -> Result<Self, Error> {
        let inner = parse_users(config.users, config.roles)?;
        Ok(Self { inner })
    }
}

impl UsersStorage for UsersMap {
    fn get_user<'a>(&'a self, username: &str) -> Result<&'a User, Error> {
        self.inner.get(username).ok_or(Error::UserNotFound)
    }
}
