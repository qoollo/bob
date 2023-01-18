#[macro_use]
extern crate log;
#[macro_use]
extern crate serde;

mod authenticator;
mod credentials;
mod error;
mod extractor;
mod permissions;
mod settings;
mod token;

pub use authenticator::{
    basic::Basic as BasicAuthenticator, stub::Stub as StubAuthenticator, Authenticator, UsersMap,
    AuthenticationType,
};
pub use credentials::{DeclaredCredentials, CredentialsHolder};
pub use error::Error;
pub use extractor::Extractor;
pub use permissions::Permissions;

pub const USERS_MAP_FILE: &str = "users.yaml";
