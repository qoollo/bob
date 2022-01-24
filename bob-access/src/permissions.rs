use bitflags::bitflags;
use http::{Method, Request};

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

pub trait GetPermissions {
    fn get_permissions(&self) -> Permissions;
}

impl<T> GetPermissions for Request<T> {
    fn get_permissions(&self) -> Permissions {
        warn!("@TODO differentiate GRPC and REST requests");
        match *self.method() {
            Method::GET => Permissions::READ & Permissions::READ_REST,
            Method::PUT => Permissions::WRITE & Permissions::WRITE_REST,
            _ => Permissions::FORBIDDEN,
        }
    }
}
