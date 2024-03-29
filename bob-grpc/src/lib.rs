#[macro_use]
extern crate log;

pub use grpc::*;

/// Autogenerated code from proto
pub mod grpc {
    include!("bob_storage.rs");
}

impl PutOptions {
    pub fn new_local() -> Self {
        PutOptions {
            remote_nodes: vec![],
            force_node: true,
            overwrite: false,
        }
    }

    pub fn new_alien(remote_nodes: Vec<String>) -> Self {
        PutOptions {
            remote_nodes,
            force_node: true,
            overwrite: false,
        }
    }
}

impl GetOptions {
    pub fn new_local() -> Self {
        GetOptions {
            force_node: true,
            source: GetSource::Normal as i32,
        }
    }

    pub fn new_alien() -> Self {
        GetOptions {
            force_node: true,
            source: GetSource::Alien as i32,
        }
    }

    pub fn new_all() -> Self {
        GetOptions {
            force_node: true,
            source: GetSource::All as i32,
        }
    }
}

impl DeleteOptions {
    pub fn new_local() -> Self {
        Self {
            force_alien_nodes: vec![],
            force_node: true,
            is_alien: false
        }
    }

    pub fn new_alien(force_alien_nodes: Vec<String>) -> Self {
        Self {
            force_alien_nodes,
            force_node: true,
            is_alien: true,
        }
    }
}

impl From<i32> for GetSource {
    fn from(value: i32) -> Self {
        match value {
            0 => GetSource::All,
            1 => GetSource::Normal,
            2 => GetSource::Alien,
            other => {
                error!("cannot convert value: {} to 'GetSource' enum", other);
                panic!("fatal core error");
            }
        }
    }
}
