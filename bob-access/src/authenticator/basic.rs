use std::{collections::HashMap, net::IpAddr};

use crate::{credentials::Credentials, error::Error, permissions::Permissions};

use super::{users_storage::UsersStorage, Authenticator};

#[derive(Debug, Default, Clone)]
pub struct Basic<Storage: UsersStorage> {
    users_storage: Storage,
    nodes: HashMap<IpAddr, Credentials>,
}

impl<Storage: UsersStorage> Basic<Storage> {
    pub fn new(users_storage: Storage) -> Self {
        Self {
            users_storage,
            nodes: HashMap::new(),
        }
    }

    pub fn set_nodes_credentials(
        &mut self,
        nodes: HashMap<IpAddr, Credentials>,
    ) -> Result<(), Error> {
        if nodes
            .values()
            .all(|cred| cred.ip().is_some() && cred.username().is_some())
        {
            self.nodes = nodes;
            Ok(())
        } else {
            let message = "nodes credentials missing ip or username";
            Err(Error::CredentialsNotProvided(message.to_string()))
        }
    }

    fn is_node_request(&self, other: &Credentials) -> Option<bool> {
        if self.nodes.is_empty() {
            warn!("nodes credentials not set");
        }
        self.nodes
            .get(other.ip().as_ref()?)
            .map(|cred| cred.username() == other.username())
    }
}

impl<Storage> Authenticator for Basic<Storage>
where
    Storage: UsersStorage,
{
    fn check_credentials(&self, credentials: Credentials) -> Result<Permissions, Error> {
        debug!("check {:?}", credentials);
        if self.is_node_request(&credentials) == Some(true) {
            debug!("request from node: {:?}", credentials.username());
            return Ok(Permissions::all());
        }
        debug!(
            "external request ip: {:?}, name: {:?}",
            credentials.ip(),
            credentials.username()
        );

        match (credentials.username(), credentials.password()) {
            (Some(username), Some(password)) => {
                let user = self.users_storage.get_user(username)?;
                if user.password() == password {
                    Ok(user.into())
                } else {
                    Err(Error::UnauthorizedRequest)
                }
            }
            (Some(_), None) => Err(Error::CredentialsNotProvided(
                "missing password".to_string(),
            )),
            (None, Some(_)) => Err(Error::CredentialsNotProvided(
                "missing username".to_string(),
            )),
            (None, None) => {
                if let Ok(default_user) = self.users_storage.get_user("default") {
                    Ok(default_user.into())
                } else {
                    Err(Error::CredentialsNotProvided(
                        "missing credentials".to_string(),
                    ))
                }
            }
        }
    }
}
