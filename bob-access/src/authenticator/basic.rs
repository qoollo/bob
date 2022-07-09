use std::{collections::HashMap, net::IpAddr};

use crate::{credentials::{Credentials, CredentialsKind}, AuthenticationType, error::Error, permissions::Permissions};

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
            .all(|cred| 
                cred.ip().is_some() &&
                cred.kind().map(|k| k.is_internode()) == Some(true))
        {
            self.nodes = nodes;
            Ok(())
        } else {
            let message = "nodes credentials missing ip or node name";
            Err(Error::CredentialsNotProvided(message.to_string()))
        }
    }

    fn check_node_request(&self, node_name: &String, ip: Option<IpAddr>) -> Option<bool> {
        if self.nodes.is_empty() {
            warn!("nodes credentials not set");
        }
        self.nodes
            .get(ip.as_ref()?)
            .map(|cred|
                if let Some(CredentialsKind::InterNode(other_name)) = cred.kind() {
                    node_name == other_name
                } else {
                    false
                })
    }

    fn check_credentials_common(&self, credentials: Credentials) -> Result<Permissions, Error> {
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
            None => {
                if let Ok(default_user) = self.users_storage.get_user("default") {
                    Ok(default_user.into())
                } else {
                    Err(Error::CredentialsNotProvided(
                        "missing credentials".to_string(),
                    ))
                }
            },
            _ => Err(Error::UnauthorizedRequest),
        }
    }
}

impl<Storage> Authenticator for Basic<Storage>
where
    Storage: UsersStorage,
{
    fn check_credentials_grpc(&self, credentials: Credentials) -> Result<Permissions, Error> {
        debug!("check {:?}", credentials);
        match credentials.kind() {
            Some(CredentialsKind::InterNode(node_name)) => {
                if let Some(true) = self.check_node_request(node_name, credentials.ip()) {
                    debug!("request from node: {:?}", credentials.ip());
                    Ok(Permissions::all())
                } else {
                    Err(Error::UnauthorizedRequest)
                }
            },
            _ => self.check_credentials_common(credentials),
        }
    }

    fn check_credentials_rest(&self, credentials: Credentials) -> Result<Permissions, Error> {
        debug!("check {:?}", credentials);
        self.check_credentials_common(credentials)
    }

    fn credentials_type() -> AuthenticationType {
        AuthenticationType::Basic
    }
}
