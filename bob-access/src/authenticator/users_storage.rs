use crate::error::Error;
use serde::Deserialize;

pub mod hashmap;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Perms {
    #[serde(default)]
    read: bool,
    #[serde(default)]
    write: bool,
    #[serde(default)]
    read_rest: bool,
    #[serde(default)]
    write_rest: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct User {
    username: String,
    password: String,
    perms: Perms,
}

impl User {
    pub fn new(username: String, password: String, perms: Perms) -> Self {
        Self {
            username,
            password,
            perms,
        }
    }

    pub fn password(&self) -> &str {
        &self.password
    }

    pub fn can_read(&self) -> bool {
        self.perms.read
    }

    pub fn can_write(&self) -> bool {
        self.perms.write
    }

    pub fn can_read_rest(&self) -> bool {
        self.perms.read_rest
    }

    pub fn can_write_rest(&self) -> bool {
        self.perms.write_rest
    }
}

pub trait UsersStorageTrait: Default + Clone {
    fn get_user<'a>(&'a self, username: &str) -> Result<&'a User, Error>;
}
