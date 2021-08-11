#[macro_use]
extern crate serde_derive;

mod blob_recovery_tool;
use blob_recovery_tool as brt;

fn main() {
    brt::utils::init_logger().unwrap();
    log::info!("Logger initialized");
    brt::command::MainCommand::run().unwrap();
}
