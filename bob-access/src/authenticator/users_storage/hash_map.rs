use std::{collections::HashMap, fs::File};

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
    pub fn from_file(filename: &str) -> Result<Self, Error> {
        let f = File::open(filename)
            .map_err(|e| Error::os(format!("Can't open file {} (reason: {})", filename, e)))?;
        let users_config: ConfigUsers = serde_yaml::from_reader(f).map_err(|e| {
            Error::validation(format!("Can't parse users config file (reason: {})", e))
        })?;
        let inner = parse_users(users_config.users, users_config.roles)?;
        Ok(Self { inner })
    }
}

impl UsersStorage for UsersMap {
    fn get_user<'a>(&'a self, username: &str) -> Result<&'a User, Error> {
        self.inner.get(username).ok_or_else(Error::user_not_found)
    }
}
