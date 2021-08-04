use super::super::Perms;
use super::User;
use crate::error::Error;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize)]
pub(super) struct ConfigUser {
    pub(super) username: String,
    pub(super) password: String,
    pub(super) role: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct ConfigUsers {
    pub(super) roles: HashMap<String, Perms>,
    pub(super) users: Vec<ConfigUser>,
}

pub(super) fn parse_users(
    yaml_users: Vec<ConfigUser>,
    roles: HashMap<String, Perms>,
) -> Result<HashMap<String, User>, Error> {
    let mut users = HashMap::new();
    yaml_users.into_iter().try_for_each(|u| {
        let &perms = roles
            .get(&u.role)
            .ok_or_else(|| Error::validation(format!("Can't find role {}", u.role)))?;
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
