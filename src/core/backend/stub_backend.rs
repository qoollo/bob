use crate::core::backend::backend::*;
use crate::core::data::{BobData, BobKey, BobMeta, VDiskId};
use futures::future::ok;

#[derive(Clone)]
pub struct StubBackend {}

impl BackendStorage for StubBackend {
    fn put(&self, _disk_name: String, _vdisk: VDiskId, key: BobKey, data: BobData) -> Put {
        debug!("PUT[{}]: hi from backend, timestamp: {}", key, data.meta);        
        Put({
            Box::new(ok(BackendResult {}))
        })
    }
    fn put_alien(&self, _vdisk: VDiskId, key: BobKey, data: BobData) -> BackendPutFuture {
        debug!("PUT[{}]: hi from backend, timestamp: {}", key, data.meta);
        Box::new(ok(BackendResult {}))
    }

    fn get(&self, _disk_name: String, _vdisk: VDiskId, key: BobKey) -> Get {
        debug!("GET[{}]: hi from backend", key);
        Get({
            Box::new(ok(BackendGetResult {
                data: BobData {
                    data: vec![0],
                    meta: BobMeta::new_stub(),
                },
            }))    
        })
    }

    fn get_alien(&self, _vdisk: VDiskId, key: BobKey) -> BackendGetFuture {
        debug!("GET[{}]: hi from backend", key);
        Box::new(ok(BackendGetResult {
            data: BobData {
                data: vec![0],
                meta: BobMeta::new_stub(),
            },
        }))
    }
}
