use std::{collections::HashMap, fs::File};

use crate::error::Error;

use super::{
    config::{parse_users, ConfigUsers},
    User, UsersStorage,
};

#[derive(Default, Clone, Debug)]
pub struct UsersMap {
    inner: HashMap<String, User>,
    password_salt: String,
}

impl UsersMap {
    pub fn from_file(filename: &str) -> Result<Self, Error> {
        let f = File::open(filename)
            .map_err(|e| Error::Os(format!("Can't open file {} (reason: {})", filename, e)))?;
        let users_config: ConfigUsers = serde_yaml::from_reader(f).map_err(|e| {
            Error::Validation(format!("Can't parse users config file (reason: {})", e))
        })?;
        let inner = parse_users(users_config.users, users_config.roles)?;
        let password_salt = users_config.password_salt;
        Ok(Self {
            inner,
            password_salt,
        })
    }
}

impl UsersStorage for UsersMap {
    fn get_user<'a>(&'a self, username: &str) -> Result<&'a User, Error> {
        self.inner.get(username).ok_or(Error::UserNotFound)
    }

    fn get_password_salt<'a>(&'a self) -> &'a str {
        &self.password_salt
    }
}
