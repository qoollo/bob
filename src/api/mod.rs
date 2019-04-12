pub mod grpc {
    #![allow(clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/bob_storage.rs"));
}
