pub mod basic;
pub mod stub;
mod users_storage;

use crate::{credentials::Credentials, error::Error, permissions::Permissions, CredentialsType};

pub use users_storage::{User, UsersMap};

pub trait Authenticator: Clone + Send + Sync + 'static {
    fn check_credentials_rest(&self, credentials: Credentials) -> Result<Permissions, Error>;
    fn check_credentials_grpc(&self, credentials: Credentials) -> Result<Permissions, Error>;
    fn credentials_type() -> CredentialsType;
}
