#[allow(clippy::needless_pass_by_value)] // allow for http mod, because of rocket lib macro impl
pub mod http;

use crate::server::Server as BobServer;

use bob_common::configs::{Access as AccessConfig, User as UserConfig};
use rocket::{
    http::Status,
    request::{FromRequest, Outcome},
    Request,
};

const BOB_API_USER_HEADER: &str = "x-bob-user-key";
const BOB_API_PASSWORD_HEADER: &str = "x-bob-user-key";

#[derive(Debug)]
pub struct Api {
    bob: BobServer,
    users: Vec<User>,
}

#[derive(Debug)]
struct User {
    name: String,
    password: String,
    access: Access,
}

impl<'a, 'r> FromRequest<'a, 'r> for User {
    type Error = Error;
    fn from_request(request: &'a Request<'r>) -> Outcome<Self, Self::Error> {
        let keys: Vec<_> = request.headers().get(BOB_API_USER_HEADER).collect();
        let name = match keys.len() {
            0 => return Outcome::Failure((Status::BadRequest, Error::MissingUser)),
            1 => keys[0].to_string(),
            _ => return Outcome::Failure((Status::BadRequest, Error::BadHeadersCount)),
        };

        let user = User {
            name,
            password,
            access,
        };

        Outcome::Success(user)
    }
}

impl From<UserConfig> for User {
    fn from(config: UserConfig) -> Self {
        let access = match config.http_perms() {
            AccessConfig::Read => Access::Read,
            AccessConfig::Write => Access::Write,
        };
        Self {
            name: config.name().to_owned(),
            password: config.name().to_owned(),
            access,
        }
    }
}

#[derive(Debug)]
enum Access {
    Read,
    Write,
}

#[derive(Debug)]
enum Error {
    BadHeadersCount,
    MissingUser,
    MissingPassword,
    PermissionDenied,
    UnknownUser,
    WrongPassword,
}
