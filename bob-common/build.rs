use std::env;
use std::fs::File;
use std::io::Write;
use std::path::Path;

fn main() {
    let out_dir = env::var("OUT_DIR").expect("No OUD_DIR");
    let dest_path = Path::new(&out_dir).join("key_constants.rs");
    let mut f = File::create(&dest_path).expect("Could not create file");
    let key_size = option_env!("BOB_KEY_SIZE");
    let key_size = key_size
        .map_or(Ok(8), str::parse)
        .expect("Could not parse BOB_KEY_SIZE");

    write!(&mut f, "const BOB_KEY_SIZE: usize = {};", key_size).expect("Could not write file");
}
