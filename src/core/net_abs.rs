use crate::core::data::{BobKey, BobData, Node, BobPutResult, BobErrorResult};
use futures::future::Future;

use crate::api::grpc::{PutRequest,GetRequest};

#[derive(Clone)]
pub struct BobClient {
    node: Node
}

impl BobClient {
    pub fn new(node: Node) -> Self {
        Self {
            node: node
        }
    }

    pub fn put(&self, key: &BobKey, data: &BobData) -> impl Future<Item=BobPutResult, Error=BobErrorResult> {
        futures::future::ok(BobPutResult {})
    }
}