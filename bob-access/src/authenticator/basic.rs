use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{RwLock, Arc},
    time::Duration,
};

use crate::{credentials::{DeclaredCredentials, DCredentialsResolveGuard, RequestCredentials, CredentialsKind}, AuthenticationType, error::Error, permissions::Permissions};

use super::{users_storage::UsersStorage, Authenticator};

use sha2::{Digest, Sha512};

use tokio::net::lookup_host;

#[inline]
async fn lookup(hostname: &str) -> Option<Vec<SocketAddr>> {
    lookup_host(hostname).await
    .ok()
    .map(|addr| addr.collect())
}

type NodesCredentials = HashMap<String, DCredentialsResolveGuard>;

#[derive(Debug, Default, Clone)]
pub struct Basic<Storage: UsersStorage> {
    users_storage: Storage,
    nodes: Arc<RwLock<NodesCredentials>>,
    resolve_sleep_dur_ms: u64,
}

impl<Storage: UsersStorage> Basic<Storage> {
    pub fn new(users_storage: Storage, resolve_sleep_dur_ms: u64) -> Self {
        Self {
            users_storage,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            resolve_sleep_dur_ms,
        }
    }

    fn node_creds_ok(creds: &HashMap<String, DeclaredCredentials>) -> bool {
        creds.values()
            .all(|cred| cred.validate_internode())
    }

    pub fn set_nodes_credentials(
        &mut self,
        mut nodes: HashMap<String, DeclaredCredentials>,
    ) -> Result<(), Error> {
        if Self::node_creds_ok(&nodes) {
            let mut nodes_creds = self.nodes.write().expect("nodes credentials lock");
            for (nodename, cred) in nodes.drain() {
                let guard = DCredentialsResolveGuard::new(cred.clone());
                nodes_creds.insert(nodename, guard);
                if cred.ip().is_empty() && cred.hostname().is_some() {
                    self.spawn_resolver(cred);
                }
            }
            Ok(())
        } else {
            let message = "nodes credentials missing ip or node name";
            Err(Error::CredentialsNotProvided(message.to_string()))
        }
    }

    fn spawn_resolver(&self, cred: DeclaredCredentials) {
        tokio::spawn(Self::resolve_worker(self.nodes.clone(), cred, self.resolve_sleep_dur_ms));
    }

    async fn resolve_worker(nodes: Arc<RwLock<NodesCredentials>>, cred: DeclaredCredentials, sleep_dur_ms: u64) {
        let hostname = cred.hostname().as_ref().expect("resolve worker without hostname");
        let mut addr: Option<Vec<SocketAddr>> = lookup(hostname).await;
        let mut cur_sleep_dur_ms = 100;
        while addr.is_none() || (addr.is_some() && addr.as_ref().unwrap().len() == 0) {
            tokio::time::sleep(Duration::from_millis(cur_sleep_dur_ms)).await;

            addr = lookup(hostname).await;

            cur_sleep_dur_ms = sleep_dur_ms.min(cur_sleep_dur_ms * 2);
        }

        let addr = addr.expect("somehow addr is none");
        if let CredentialsKind::InterNode(nodename) = cred.kind() {
            let mut nodes = nodes.write().expect("nodes credentials lock");
            if let Some(creds) = nodes.get_mut(nodename) {
                creds.creds_mut().replace_addresses(addr);
                creds.set_resolved();
            }
        } else {
            error!("resolved credentials are not internode");
        }
    }

    fn process_auth_result(&self, nodename: &str, unresolved: bool) {
        let mut nodes = self.nodes.write().expect("nodes credentials lock");
        if let Some(node) = nodes.get_mut(nodename) {
            if node.update_resolve_state(unresolved) {
                node.set_pending();
                self.spawn_resolver(node.creds().clone());
            }
        }
    }

    fn check_node_request(&self, node_name: &String, ip: Option<SocketAddr>) -> bool {
        let mut unresolved = None;
        {
            if ip.is_none() {
                return false;
            }
            let ip = ip.unwrap().ip();
            let nodes = self.nodes.read().expect("nodes credentials lock");
            if let Some(cred) = nodes.get(node_name).map(|guard| guard.creds()) {
                unresolved = Some(false);
                if let CredentialsKind::InterNode(other_name) = cred.kind() {
                    debug_assert!(node_name == other_name);
                    if cred.ip().iter().find(|cred_ip| cred_ip.ip() == ip).is_some() {
                        return true;
                    }
                    
                    unresolved = Some(true);
                }
            }
        }
        unresolved.map(|unresolved| 
            self.process_auth_result(node_name, unresolved));
        false
    }

    fn check_credentials_common(&self, credentials: RequestCredentials) -> Result<Permissions, Error> {
        match credentials.kind() {
            Some(CredentialsKind::Basic { username, password }) => {
                debug!(
                    "external request ip: {:?}, name: {:?}",
                    credentials.ip(),
                    username
                );
        
                let user = self.users_storage.get_user(&username)?;
                if let Some(usr_password) = user.password() {
                    if usr_password == password {
                        Ok(user.into())
                    } else {
                        Err(Error::UnauthorizedRequest)
                    }
                } else if let Some(usr_hash) = user.password_hash() {
                    let hash_str = format!("{}{}", password, self.users_storage.get_password_salt());
                    let mut hasher = Sha512::new();
                    hasher.update(&hash_str.into_bytes());
                    let hash = hasher.finalize();
                    if hash[..] == usr_hash[..] {
                        Ok(user.into())
                    } else {
                        Err(Error::UnauthorizedRequest)
                    }
                } else {
                    Err(Error::UnauthorizedRequest)
                }
            },
            None => {
                Err(Error::CredentialsNotProvided("missing credentials".to_string()))
            },
            _ => Err(Error::UnauthorizedRequest),
        }
    }
}

impl<Storage> Authenticator for Basic<Storage>
where
    Storage: UsersStorage,
{
    fn check_credentials_grpc(&self, credentials: RequestCredentials) -> Result<Permissions, Error> {
        debug!("check {:?}", credentials);
        match credentials.kind() {
            Some(CredentialsKind::InterNode(node_name)) => {
                if self.check_node_request(node_name, credentials.ip()) {
                    debug!("request from node: {:?}", credentials.ip());
                    Ok(Permissions::all())
                } else {
                    Err(Error::UnauthorizedRequest)
                }
            },
            _ => self.check_credentials_common(credentials),
        }
    }

    fn check_credentials_rest(&self, credentials: RequestCredentials) -> Result<Permissions, Error> {
        debug!("check {:?}", credentials);
        self.check_credentials_common(credentials)
    }

    fn credentials_type() -> AuthenticationType {
        AuthenticationType::Basic
    }
}
