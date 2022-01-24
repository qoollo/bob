pub mod basic;
pub mod stub;
mod users_storage;

use crate::{credentials::Credentials, error::Error, permissions::Permissions};

pub use users_storage::{User, UsersMap};

pub trait Authenticator: Clone {
    fn check_credentials(&self, credentials: Credentials) -> Result<Permissions, Error>;
}
