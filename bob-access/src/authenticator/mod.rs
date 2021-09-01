pub mod basic;
pub mod stub;
mod users_storage;

use crate::{credentials::Credentials, error::Error};

pub use users_storage::UsersMap;

pub trait Authenticator: Clone {
    fn check_credentials(&self, credentials: Credentials) -> Result<(), Error>;
}
