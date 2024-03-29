use bitflags::bitflags;
use std::fmt::Display;

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

impl Permissions {
    pub fn has_rest_read(&self) -> bool {
        self.contains(Self::READ_REST)
    }

    pub fn has_rest_write(&self) -> bool {
        self.contains(Self::WRITE_REST)
    }

    pub fn has_write(&self) -> bool {
        self.contains(Self::WRITE)
    }

    pub fn has_read(&self) -> bool {
        self.contains(Self::READ)
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
