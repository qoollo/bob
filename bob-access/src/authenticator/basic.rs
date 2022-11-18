use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{RwLock, Arc},
    thread::sleep, 
    time::Duration,
};

use crate::{credentials::{Credentials, CredentialsKind}, AuthenticationType, error::Error, permissions::Permissions};

use super::{users_storage::UsersStorage, Authenticator};

use sha2::{Digest, Sha512};

use tokio::net::lookup_host;

type NodesCredentials = HashMap<String, Credentials>;

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

    fn node_creds_ok(creds: &HashMap<String, Credentials>) -> bool {
        creds.values()
            .all(|cred|
                cred.ip().is_some() &&
                cred.kind().map(|k| k.is_internode()) == Some(true))
    }

    fn unresolved_node_creds_ok(creds: &Vec<Credentials>) -> bool {
        creds.iter()
            .all(|cred|
                cred.hostname().is_some() &&
                cred.kind().map(|k| k.is_internode()) == Some(true))
    }

    pub fn set_nodes_credentials(
        &mut self,
        nodes: HashMap<String, Credentials>,
        unresolved: Vec<Credentials>,
    ) -> Result<(), Error> {
        if Self::node_creds_ok(&nodes) && Self::unresolved_node_creds_ok(&unresolved)
        {
            {
                let mut nodes_creds = self.nodes.write().expect("nodes credentials lock");
                *nodes_creds = nodes;
            }
            for cred in unresolved {
                self.spawn_resolver(cred);
            }
            Ok(())
        } else {
            let message = "nodes credentials missing ip or node name";
            Err(Error::CredentialsNotProvided(message.to_string()))
        }
    }

    fn spawn_resolver(&self, cred: Credentials) {
        tokio::spawn(Self::resolve_worker(self.nodes.clone(), cred, self.resolve_sleep_dur_ms));
    }

    async fn resolve_worker(nodes: Arc<RwLock<NodesCredentials>>, mut cred: Credentials, sleep_dur_ms: u64) {
        let hostname = cred.hostname().as_ref().expect("resolve worker without hostname");
        let mut addr: Option<Vec<SocketAddr>> = None;
        for _ in 0..60 {
            addr = match lookup_host(hostname).await {
                Ok(address) => Some(address.collect()),
                _ => None
            };
            if let Some(addr) = addr.as_ref() {
                if addr.len() > 0 {
                    break;
                }
            }

            sleep(Duration::from_secs(1));
        }

        while addr.is_none() || (addr.is_some() && addr.as_ref().unwrap().len() == 0) {
            sleep(Duration::from_millis(sleep_dur_ms));

            addr = match lookup_host(hostname).await {
                Ok(address) => Some(address.collect()),
                _ => None
            };
        }

        let addr = addr.expect("somehow addr is none");
        cred.set_addresses(addr);
        let mut nodes = nodes.write().expect("nodes credentials lock");
        if let Some(CredentialsKind::InterNode(nodename)) = cred.kind() {
            nodes.insert(nodename.clone(), cred);
        } else {
            error!("resolved credentials are not internode");
        }
    }

    fn mark_unresolved(&self, nodename: &str) {
        let mut nodes = self.nodes.write().expect("nodes credentials lock");
        let cred = nodes.remove(nodename);
        cred.map(|cred| self.spawn_resolver(cred));
    }

    fn check_node_request(&self, node_name: &String, ip: Option<SocketAddr>) -> bool {
        let mut unresolved = false;
        {
            if ip.is_none() {
                return false;
            }
            let nodes = self.nodes.read().expect("nodes credentials lock");
            if nodes.is_empty() {
                warn!("nodes credentials not set");
                return false;
            }
            let ip = ip.unwrap().ip();
            if let Some(cred) = nodes.get(node_name) {
                if let Some(CredentialsKind::InterNode(other_name)) = cred.kind() {
                    if node_name == other_name {
                        if cred.ip().as_ref().and_then(|ips| {
                            ips.iter().find(|cred_ip| cred_ip.ip() == ip)
                        }).is_some() {
                            return true;
                        }
                        
                        unresolved = true;
                    }
                }
            }
        }
        if unresolved {
            self.mark_unresolved(node_name);
        }
        false
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
    fn check_credentials_grpc(&self, credentials: Credentials) -> Result<Permissions, Error> {
        debug!("check {:?}", credentials);
        match credentials.kind() {
            Some(CredentialsKind::InterNode(node_name)) => {
                if self.check_node_request(node_name, credentials.single_ip()) {
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
