use std::env;
use std::fs::File;
use std::path::Path;
use std::io::Write;

extern crate tonic_build;

fn main() {
    let out_dir = env::var("OUT_DIR").expect("No out dir");
    let dest_path = Path::new(&out_dir).join("key_constants.rs");
    let mut f = File::create(&dest_path).expect("Could not create file");
    let key_size = option_env!("KEY_SIZE");
    let key_size = key_size
        .map_or(Ok(8), str::parse)
        .expect("Could not parse KEY_SIZE");

    write!(&mut f, "const KEY_SIZE: usize = {};", key_size)
        .expect("Could not write file");

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .format(false)
        .compile(&["proto/bob.proto"], &["proto"])
        .expect("protobuf compilation");
}
