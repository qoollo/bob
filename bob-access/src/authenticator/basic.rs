use crate::{credentials::Credentials, error::Error};

use super::{users_storage::UsersStorage, Authenticator};

#[derive(Debug, Default, Clone)]
pub struct Basic<Storage: UsersStorage> {
    users_storage: Storage,
    nodes: Vec<Credentials>,
}

impl<Storage: UsersStorage> Basic<Storage> {
    pub fn new(users_storage: Storage) -> Self {
        Self {
            users_storage,
            nodes: Vec::new(),
        }
    }

    pub fn set_nodes_credentials(&mut self, nodes: Vec<Credentials>) -> Result<(), Error> {
        if nodes
            .iter()
            .all(|cred| cred.ip().is_some() && cred.username().is_some())
        {
            self.nodes = nodes;
            Ok(())
        } else {
            let message = "nodes credentials missing ip or username";
            Err(Error::credentials_not_provided(message))
        }
    }

    fn is_node(&self, other: &Credentials) -> bool {
        if self.nodes.is_empty() {
            warn!("nodes credentials not set");
        }
        self.nodes
            .iter()
            .any(|cred| cred.ip() == other.ip() && cred.username() == other.username())
    }
}

impl<Storage: UsersStorage> Authenticator for Basic<Storage> {
    fn check_credentials(&self, credentials: Credentials) -> Result<(), Error> {
        if self.is_node(&credentials) {
            debug!("received request from node: {:?}", credentials.username());
            return Ok(());
        }
        let username = credentials
            .username()
            .ok_or_else(|| Error::credentials_not_provided("missing username"))?;
        let password = credentials
            .password()
            .ok_or_else(|| Error::credentials_not_provided("missing password"))?;

        let user = self.users_storage.get_user(username)?;
        if user.password() == password {
            Ok(())
        } else {
            Err(Error::unauthorized_request())
        }
    }
}
