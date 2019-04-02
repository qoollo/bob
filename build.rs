extern crate tower_grpc_build;

fn main() {
    // Build helloworld
    tower_grpc_build::Config::new()
        .enable_server(true)
        .enable_client(true)
        .build(&["proto/bob.proto"], &["proto/"])
        .unwrap_or_else(|e| panic!("protobuf compilation failed: {}", e));
}
