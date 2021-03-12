pub mod bob_client;
pub mod configs;
pub mod data;
pub mod error;
pub mod mapper;
pub mod metrics;
pub mod node;

#[macro_use]
extern crate bitflags;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
// #[macro_use]
// extern crate cfg_if;
#[macro_use]
extern crate metrics as lib_metrics;

mod prelude {
    pub use std::path::Path;

    pub use anyhow::{Context, Result as AnyResult};
}
