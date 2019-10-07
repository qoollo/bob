use super::prelude::*;

#[derive(Clone)]
pub struct StubBackend {}

impl BackendStorage for StubBackend {
    fn run_backend(&self) -> RunResult {
        async move { Ok(()) }.boxed()
    }

    fn put(&self, _operation: BackendOperation, key: BobKey, data: BobData) -> Put {
        debug!("PUT[{}]: hi from backend, timestamp: {}", key, data.meta);
        Put(future::ok(BackendPutResult {}).boxed())
    }

    fn put_alien(&self, _operation: BackendOperation, key: BobKey, data: BobData) -> Put {
        debug!("PUT[{}]: hi from backend, timestamp: {}", key, data.meta);
        Put(future::ok(BackendPutResult {}).boxed())
    }

    fn get(&self, _operation: BackendOperation, key: BobKey) -> Get {
        debug!("GET[{}]: hi from backend", key);
        Get(future::ok(BackendGetResult {
            data: BobData::new(vec![0], BobMeta::new_stub()),
        })
        .boxed())
    }

    fn get_alien(&self, _operation: BackendOperation, key: BobKey) -> Get {
        debug!("GET[{}]: hi from backend", key);
        Get(future::ok(BackendGetResult {
            data: BobData::new(vec![0], BobMeta::new_stub()),
        })
        .boxed())
    }
}
