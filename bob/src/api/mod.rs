#[allow(clippy::needless_pass_by_value)] // allow for http mod, because of rocket lib macro impl
pub mod http;

use crate::server::Server as BobServer;

use bob_common::configs::User;

pub struct Api {
    bob: BobServer,
    users: Vec<User>,
}
