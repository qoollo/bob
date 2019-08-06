#![crate_type = "lib"]
#![feature(async_await)]
#![allow(clippy::needless_lifetimes)]

#[macro_use]
extern crate bitflags;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

pub mod api;
pub mod core;

extern crate dipstick;
