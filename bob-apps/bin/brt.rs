#[macro_use]
extern crate log;

mod blob_recovery_tool;
use blob_recovery_tool as brt;

fn main() {
    brt::utils::init_logger().unwrap();
    info!("Logger initialized");
    brt::command::MainCommand::run().unwrap();
}
