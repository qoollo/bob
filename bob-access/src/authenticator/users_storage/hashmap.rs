use super::{Perms, User, UsersStorageTrait};
use crate::error::Error;
use serde::Deserialize;
use std::{collections::HashMap, fs::File};

pub(crate) struct UsersMap {
    inner: HashMap<String, User>,
}

impl UsersMap {
    pub(crate) fn from_file(filename: &str) -> Result<Self, Error> {
        let f = File::open(filename).map_err(|e| {
            Error::validation(format!(
                "Can't open file {} (reason: {})",
                filename,
                e.to_string()
            ))
        })?;
        let users_config: UsersConfig = serde_yaml::from_reader(f).map_err(|e| {
            Error::validation(format!(
                "Can't parse users config file (reason: {})",
                e.to_string()
            ))
        })?;
        let inner = parse_users(users_config.users, users_config.roles)?;
        Ok(Self { inner })
    }
}

impl UsersStorageTrait for UsersMap {
    fn get_user<'a>(&'a self, username: &str) -> Result<&'a User, Error> {
        self.inner.get(username).ok_or_else(|| Error::not_found())
    }
}

#[derive(Debug, Clone, Deserialize)]
struct ConfigUser {
    username: String,
    password: String,
    role: String,
}

#[derive(Debug, Deserialize)]
struct UsersConfig {
    roles: HashMap<String, Perms>,
    users: Vec<ConfigUser>,
}

fn parse_users(
    yaml_users: Vec<ConfigUser>,
    roles: HashMap<String, Perms>,
) -> Result<HashMap<String, User>, Error> {
    let mut users = HashMap::new();
    yaml_users.into_iter().try_for_each(|u| {
        let perms = roles
            .get(&u.role)
            .ok_or_else(|| Error::validation(format!("Can't find role {}", u.role)))?
            .clone();
        let user = User::new(u.username.clone(), u.password, perms);
        users.insert(u.username, user).map_or(Ok(()), |user| {
            Err(Error::validation(format!(
                "Users with the same username (first: {:?})",
                user
            )))
        })
    })?;
    Ok(users)
}
