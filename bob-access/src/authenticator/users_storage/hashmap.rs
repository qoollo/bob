use super::{User, UsersStorage};
use crate::error::Error;
use std::{collections::HashMap, fs::File};

mod config;
use config::{parse_users, ConfigUsers};

#[derive(Default, Clone, Debug)]
pub struct UsersMap {
    inner: HashMap<String, User>,
}

impl UsersMap {
    pub fn from_file(filename: &str) -> Result<Self, Error> {
        let f = File::open(filename).map_err(|e| {
            Error::os(format!(
                "Can't open file {} (reason: {})",
                filename,
                e.to_string()
            ))
        })?;
        let users_config: ConfigUsers = serde_yaml::from_reader(f).map_err(|e| {
            Error::validation(format!(
                "Can't parse users config file (reason: {})",
                e.to_string()
            ))
        })?;
        let inner = parse_users(users_config.users, users_config.roles)?;
        Ok(Self { inner })
    }
}

impl UsersStorage for UsersMap {
    fn get_user<'a>(&'a self, username: &str) -> Result<&'a User, Error> {
        self.inner.get(username).ok_or_else(|| Error::not_found())
    }
}
