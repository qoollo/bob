use std::path::PathBuf;
use prost_build::Config;

extern crate tonic_build;
extern crate prost_build;

fn main() {
    let path: PathBuf = format!("{}/src", env!("CARGO_MANIFEST_DIR")).into();
    let mut prost_config = Config::new();
    prost_config.bytes(&["Blob.data"]);
    if !path.join("bob_storage.rs").exists() {
        tonic_build::configure()
            .build_server(true)
            .build_client(true)
            .format(false)
            .out_dir(format!("{}/src", env!("CARGO_MANIFEST_DIR")))
            .compile_with_config(prost_config, &["proto/bob.proto"], &["proto"])
            .expect("protobuf compilation");
    }
}
