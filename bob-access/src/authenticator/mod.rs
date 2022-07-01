pub mod basic;
pub mod stub;
mod users_storage;

use crate::{credentials::Credentials, error::Error, permissions::Permissions};

pub use users_storage::{User, UsersMap};

pub trait Authenticator: Clone + Send + Sync + 'static {
    fn check_credentials_rest(&self, credentials: Credentials) -> Result<Permissions, Error>;
    fn check_credentials_grpc(&self, credentials: Credentials) -> Result<Permissions, Error>;
    fn credentials_type() -> AuthenticationType;
}

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
pub enum AuthenticationType {
    None,
    Basic,
    Token,
}

impl AuthenticationType {
    pub fn is_basic(&self) -> bool {
        *self == AuthenticationType::Basic
    }

    pub fn is_stub(&self) -> bool {
        *self == AuthenticationType::None
    }
}