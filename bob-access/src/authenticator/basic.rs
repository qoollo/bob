use std::{collections::HashMap, net::IpAddr};

use crate::{credentials::{Credentials, CredentialsKind, CredentialsType}, error::Error, permissions::Permissions};

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
            .all(|cred| cred.ip().is_some() && cred.kind().is_some())
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
            .map(|cred| 
                match (cred.kind(), other.kind()) {
                    (Some(CredentialsKind::Basic { username: c_username, password: _ }),
                    Some(CredentialsKind::Basic { username: o_username, password: _ })) => c_username == o_username,
                    (Some(CredentialsKind::Token(c_token)), 
                    Some(CredentialsKind::Token(o_token))) => c_token == o_token,
                    _ => false
                })
    }
}

impl<Storage> Authenticator for Basic<Storage>
where
    Storage: UsersStorage,
{
    fn check_credentials(&self, credentials: Credentials) -> Result<Permissions, Error> {
        debug!("check {:?}", credentials);
        if self.is_node_request(&credentials) == Some(true) {
            debug!("request from node: {:?}", credentials.ip());
            return Ok(Permissions::all());
        }
        match credentials.kind() {
            Some(CredentialsKind::Basic { username, password }) => {
                debug!(
                    "external request ip: {:?}, name: {:?}",
                    credentials.ip(),
                    username
                );
        
                let user = self.users_storage.get_user(&username)?;
                if user.password() == password {
                    Ok(user.into())
                } else {
                    Err(Error::UnauthorizedRequest)
                }
            },
            Some(CredentialsKind::Token(_token)) => {
                debug!(
                    "external token request ip: {:?}",
                    credentials.ip());
                // todo
                Err(Error::UnauthorizedRequest)
            },
            None => {
                Err(Error::CredentialsNotProvided("missing credentials".to_string()))
            }
        }
    }

    fn credentials_type() -> CredentialsType {
        CredentialsType::Basic
    }
}
