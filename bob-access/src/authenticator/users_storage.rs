use crate::error::Error;
use serde::Deserialize;

mod hashmap;

#[derive(Debug, Clone, Deserialize, Default)]
struct Perms {
    #[serde(default)]
    read: bool,
    #[serde(default)]
    write: bool,
    #[serde(default)]
    read_rest: bool,
    #[serde(default)]
    write_rest: bool,
}

#[derive(Debug)]
struct User {
    username: String,
    password: String,
    perms: Perms,
}

impl User {
    fn new(username: String, password: String, perms: Perms) -> Self {
        Self {
            username,
            password,
            perms,
        }
    }

    fn password(&self) -> &str {
        &self.password
    }

    fn can_read(&self) -> bool {
        self.perms.read
    }

    fn can_write(&self) -> bool {
        self.perms.write
    }

    fn can_read_rest(&self) -> bool {
        self.perms.read_rest
    }

    fn can_write_rest(&self) -> bool {
        self.perms.write_rest
    }
}

trait UsersStorageTrait {
    fn get_user<'a>(&'a self, username: &str) -> Result<&'a User, Error>;
}
