use std::fmt::Display;

use bitflags::bitflags;
use http::{HeaderValue, Method, Request};

use crate::authenticator::User;

bitflags! {
    pub struct Permissions: u8 {
        const READ = 0b00000001;
        const WRITE = 0b00000010;
        const READ_REST = 0b00000100;
        const WRITE_REST = 0b00001000;
        const FORBIDDEN = 0b10000000;
    }
}

impl From<&User> for Permissions {
    fn from(user: &User) -> Self {
        let mut p = Self::empty();
        if user.can_read() {
            p.set(Self::READ, true);
        }
        if user.can_write() {
            p.set(Self::WRITE, true);
        }
        if user.can_read_rest() {
            p.set(Self::READ_REST, true);
        }
        if user.can_write_rest() {
            p.set(Self::WRITE_REST, true);
        }
        p
    }
}

impl Display for Permissions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GRPC: [read: {}, write: {}], REST: [read: {}, write: {}]",
            self.contains(Self::READ),
            self.contains(Self::WRITE),
            self.contains(Self::READ_REST),
            self.contains(Self::WRITE_REST)
        )
    }
}

pub trait GetRequiredPermissions {
    fn get_permissions(&self) -> Permissions;
}

impl<T> GetRequiredPermissions for Request<T>
where
    T: std::fmt::Debug,
{
    fn get_permissions(&self) -> Permissions {
        trace!("request: {:#?}", self);
        if let Some(content_type) = self.headers().get(http::header::CONTENT_TYPE) {
            let grpc_content_type = HeaderValue::from_static("application/grpc");
            if content_type == grpc_content_type {
                let perms = match *self.method() {
                    Method::GET => Permissions::READ,
                    Method::PUT | Method::DELETE | Method::POST => Permissions::WRITE,
                    _ => Permissions::FORBIDDEN,
                };
                return perms;
            }
        }
        match *self.method() {
            Method::GET => Permissions::READ_REST,
            Method::PUT | Method::DELETE | Method::POST => Permissions::WRITE_REST,
            _ => Permissions::FORBIDDEN,
        }
    }
}
