use std::collections::HashMap;

use crate::error::Error;

use super::{Perms, User};

use hex::FromHex;

#[derive(Debug, PartialEq, Copy, Clone, Deserialize, Default)]
pub struct ClaimPerms {
    read: Option<bool>,
    write: Option<bool>,
    read_rest: Option<bool>,
    write_rest: Option<bool>,
}

impl ClaimPerms {
    fn update_perms(&self, p: &mut Perms) {
        if let Some(read) = self.read {
            p.read = read;
        }
        if let Some(write) = self.write {
            p.write = write;
        }
        if let Some(read_rest) = self.read_rest {
            p.read_rest = read_rest;
        }
        if let Some(write_rest) = self.write_rest {
            p.write_rest = write_rest;
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct ConfigUser {
    pub(super) username: String,
    pub(super) password: Option<String>,
    pub(super) password_hash: Option<String>,
    pub(super) role: Option<String>,
    pub(super) claims: Option<ClaimPerms>,
}

impl ConfigUser {
    fn default_password() -> String {
        "".into()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ConfigUsers {
    pub(super) roles: HashMap<String, Perms>,
    pub(super) users: Vec<ConfigUser>,
    #[serde(default = "ConfigUsers::default_password_salt")]
    pub(super) password_salt: String,
}

impl ConfigUsers {
    fn default_password_salt() -> String {
        "bob".into()
    }
}

pub(super) fn parse_users(
    yaml_users: Vec<ConfigUser>,
    roles: HashMap<String, Perms>,
) -> Result<HashMap<String, User>, Error> {
    let mut users = HashMap::new();
    yaml_users.into_iter().try_for_each(|u| {
        let mut perms = if let Some(role) = u.role {
            *(roles
                .get(&role)
                .ok_or_else(|| Error::Validation(format!("Can't find role {}", role)))?)
        } else {
            Perms::new(false, false, false, false)
        };
        if let Some(claims) = u.claims {
            claims.update_perms(&mut perms);
        }
        let hash = u
            .password_hash
            .map(|h| Vec::from_hex(h).expect("Invalid sha512 hash"));
        if let Some(false) = hash.as_ref().map(|hash| hash.len() == 64) {
            return Err(Error::Validation(format!(
                "User's {} password_hash size is not 512 bits",
                u.username
            )));
        }
        let password = if hash.is_none() && u.password.is_none() {
            Some(ConfigUser::default_password())
        } else {
            u.password
        };
        let user = User::new(u.username.clone(), password, hash, perms);
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
