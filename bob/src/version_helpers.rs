
pub fn get_bob_version() -> String {
    format!(
        "{}-{}",
        env!("CARGO_PKG_VERSION"),
        option_env!("BOB_COMMIT_HASH").unwrap_or("hash-undefined"),
    )
}

pub fn get_bob_build_time() -> &'static str {
    crate::build_time::BUILD_TIME
}

pub fn get_pearl_version() -> String {
    pearl::get_pearl_version()
}

pub fn get_pearl_build_time() -> &'static str {
    pearl::get_pearl_build_time()
}
