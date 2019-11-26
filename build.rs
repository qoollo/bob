extern crate tonic_build;

fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&["proto/bob.proto"], &["proto"])
        .expect("protobuf compilation");
}
