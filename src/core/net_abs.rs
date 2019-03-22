use crate::core::data::{BobKey, BobData, Node};
use crate::api::bob_grpc::{BobApi, BobApiClient};
use crate::api::bob::{PutRequest, Blob, BlobKey, PutOptions};
use grpc::ClientStubExt;
use grpc::RequestOptions;
use std::sync::Arc;

#[derive(Clone)]
pub struct BobClient {
    node: Node,
    grpc_client: Arc<BobApiClient>
}

impl BobClient {
    pub fn new(node: Node) -> Result<Self, grpc::Error> {
        match BobApiClient::new_plain(&node.host, node.port, Default::default()) {
            Ok(c) => Ok(BobClient {
                    node: node,
                    grpc_client: Arc::new(c)
                }),
            Err(e) => {
                Err(e)
            }
        }
    }

    pub fn put(&self, key: &BobKey, data: &BobData) -> grpc::SingleResponse<crate::api::bob::OpStatus> {
        let mut put_req = PutRequest::new();
        put_req.set_data({
                            let mut blob = Blob::new();
                            blob.set_data(data.data.clone());
                            blob
                        });
        put_req.set_key({
            let mut bk = BlobKey::new();
            bk.set_key(key.key);
            bk
        });
        put_req.set_options({
                            let mut opts = PutOptions::new();
                            opts.set_force_node(true);
                            opts
                            });
        self.grpc_client.put(RequestOptions::new(), put_req)
    }
}