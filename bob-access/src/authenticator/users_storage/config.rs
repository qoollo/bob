use std::collections::HashMap;
use std::fs::File;

use crate::error::Error;

use super::{Perms, User};

use hex::FromHex;

#[derive(Debug, Clone, Deserialize)]
pub struct ConfigUser {
    pub(super) username: String,
    pub(super) password: Option<String>,
    pub(super) password_hash: Option<String>,
    pub(super) role: String,
}

#[derive(Debug, Deserialize)]
pub struct ConfigUsers {
    pub(super) roles: HashMap<String, Perms>,
    pub(super) users: Vec<ConfigUser>,
    #[serde(default = "ConfigUsers::default_password_salt")]
    pub password_salt: String,
}

impl ConfigUsers {
    fn default_password_salt() -> String {
        "bob".into()
    }

    pub fn from_file(path: &str) -> Result<Self, Error> {
        let f = File::open(path)
            .map_err(|e| Error::Os(format!("Can't open file {} (reason: {})", path, e)))?;
        serde_yaml::from_reader(f).map_err(|e| {
            Error::Validation(format!("Can't parse users config file (reason: {})", e))
        })
    }
}

pub(super) fn parse_users(
    yaml_users: Vec<ConfigUser>,
    roles: HashMap<String, Perms>,
) -> Result<HashMap<String, User>, Error> {
    let mut users = HashMap::new();
    yaml_users.into_iter().try_for_each(|u| {
        let &perms = roles
            .get(&u.role)
            .ok_or_else(|| Error::Validation(format!("Can't find role {}", u.role)))?;
        let hash = u.password_hash.map(|h| Vec::from_hex(h).expect("Invalid sha512 hash"));
        let user = User::new(u.username.clone(), u.password, hash, perms);
        users.insert(u.username, user).map_or(Ok(()), |user| {
            Err(Error::Validation(format!(
                "Users with the same username (first: {:?})",
                user
            )))
        })
    })?;
    Ok(users)
}

#[cfg(tests)]
mod tests {
    #[test]
    fn check_parse_users() {
        use super::*;
        let yaml_users = vec![
            ConfigUser {
                username: "Admin".to_owned(),
                password: "admin_pass".to_owned(),
                role: "admin".to_owned(),
            },
            ConfigUser {
                username: "Reader".to_owned(),
                password: "reader_pass".to_owned(),
                role: "reader".to_owned(),
            },
        ];
        let admin_perms = Perms::new(true, true, true, true);
        let reader_perms = Perms::new(true, false, true, false);
        let roles: HashMap<String, Perms> = vec![
            ("admin".to_owned(), admin_perms),
            ("reader".to_owned(), reader_perms),
        ]
        .into_iter()
        .collect();

        let users = parse_users(yaml_users, roles).expect("users with perms");
        assert_eq!(
            users.get("Admin").expect("existing user").perms,
            admin_perms
        );
        assert_eq!(
            users.get("Reader").expect("existing user").perms,
            reader_perms
        );
    }
}
