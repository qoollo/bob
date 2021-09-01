use std::path::PathBuf;

extern crate tonic_build;

fn main() {
    let path: PathBuf = format!("{}/src", env!("CARGO_MANIFEST_DIR")).into();
    if !path.join("bob_storage.rs").exists() {
        tonic_build::configure()
            .build_server(true)
            .build_client(true)
            .format(true)
            .out_dir(format!("{}/src", env!("CARGO_MANIFEST_DIR")))
            .compile(&["proto/bob.proto"], &["proto"])
            .expect("protobuf compilation");
    }
}
