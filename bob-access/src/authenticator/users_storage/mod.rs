mod config;
mod hash_map;

use crate::error::Error;

pub use hash_map::UsersMap;

#[derive(Debug, PartialEq, Copy, Clone, Deserialize, Default)]
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

impl Perms {
    pub fn new(read: bool, write: bool, read_rest: bool, write_rest: bool) -> Self {
        Self {
            read,
            write,
            read_rest,
            write_rest,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct User {
    _username: String,
    password: String,
    perms: Perms,
}

impl User {
    pub fn new(_username: String, password: String, perms: Perms) -> Self {
        Self {
            _username,
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

    pub fn perms(&self) -> Perms {
        self.perms
    }
}

pub trait UsersStorage: Default + Clone + Send + Sync + 'static {
    fn get_user<'a>(&'a self, username: &str) -> Result<&'a User, Error>;
}
