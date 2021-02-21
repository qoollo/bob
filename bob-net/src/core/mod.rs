extern crate metrics as metrics_ext;

/// Component for cleaning up memory
pub(crate) mod cleaner;
pub(crate) mod cluster;
pub(crate) mod counter;
/// Component to manage cluster I/O and connections.
pub mod grinder;
pub(crate) mod link_manager;
/// GRPC server to receive and process requests from clients.
pub mod server;

#[cfg(test)]
pub(crate) mod test_utils {
    use bob_common::{
        bob_client::{GetResult, PingResult, PutResult},
        data::BobMeta,
    };
    use chrono::Local;
    use env_logger::fmt::{Color, Formatter as EnvFormatter};
    use log::{Level, Record};
    use std::io::Result as IOResult;

    use crate::prelude::*;

    pub(crate) fn ping_ok(node_name: String) -> PingResult {
        Ok(NodeOutput::new(node_name, ()))
    }

    pub(crate) fn put_ok(node_name: String) -> PutResult {
        Ok(NodeOutput::new(node_name, ()))
    }

    pub(crate) fn put_err(node_name: String) -> PutResult {
        debug!("return internal error on PUT");
        Err(NodeOutput::new(node_name, Error::internal()))
    }

    pub(crate) fn get_ok(node_name: String, timestamp: u64) -> GetResult {
        let inner = BobData::new(vec![], BobMeta::new(timestamp));
        Ok(NodeOutput::new(node_name, inner))
    }

    pub(crate) fn get_err(node_name: String) -> GetResult {
        debug!("return internal error on GET");
        Err(NodeOutput::new(node_name, Error::internal()))
    }

    #[allow(dead_code)]
    pub(crate) fn init_logger() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Debug)
            .format(logger_format)
            .try_init();
    }

    fn logger_format(buf: &mut EnvFormatter, record: &Record) -> IOResult<()> {
        {
            let mut style = buf.style();
            let color = match record.level() {
                Level::Error => Color::Red,
                Level::Warn => Color::Yellow,
                Level::Info => Color::Green,
                Level::Debug => Color::Cyan,
                Level::Trace => Color::White,
            };
            style.set_color(color);
            writeln!(
                buf,
                "[{} {:>24}:{:^4} {:^5}] - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.module_path().unwrap_or(""),
                record.line().unwrap_or(0),
                style.value(record.level()),
                record.args(),
            )
        }
    }
}
