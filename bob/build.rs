use chrono::Local;
extern crate chrono;

fn main() {
    if cfg!(not(debug_assertions)) {
        let time = Local::now();
        let content = format!(
            "pub const BUILD_TIME: &str = \"{}\";",
            time.format("%d-%m-%Y(%H:%M:%S)")
        );
        let _ = std::fs::write(
            format!("{}/src/build_time.rs", env!("CARGO_MANIFEST_DIR")),
            content,
        );
    }
}
