use crate::core::backend::backend::*;
use crate::core::data::{BobData, BobKey, BobMeta, VDiskId};
use futures03::future::ok;
use futures03::{FutureExt};

#[derive(Clone)]
pub struct StubBackend {}

impl BackendStorage for StubBackend {
    fn put(&self, _disk_name: String, _vdisk: VDiskId, key: BobKey, data: BobData) -> Put2 {
        debug!("PUT[{}]: hi from backend, timestamp: {}", key, data.meta);
        Put2( ok(BackendResult {}).boxed() )
    }

    fn put_alien(&self, _vdisk: VDiskId, key: BobKey, data: BobData) -> Put2 {
        debug!("PUT[{}]: hi from backend, timestamp: {}", key, data.meta);
        Put2( ok(BackendResult {}).boxed() )
    }

    fn get(&self, _disk_name: String, _vdisk: VDiskId, key: BobKey) -> Get2 {
        debug!("GET[{}]: hi from backend", key);
        Get2(
            ok(BackendGetResult {
                data: BobData {
                    data: vec![0],
                    meta: BobMeta::new_stub(),
                },
            }).boxed()
        )
    }

    fn get_alien(&self, _vdisk: VDiskId, key: BobKey) -> Get2 {
        debug!("GET[{}]: hi from backend", key);
        Get2(
            ok(BackendGetResult {
                data: BobData {
                    data: vec![0],
                    meta: BobMeta::new_stub(),
                },
            }).boxed()
        )
    }
}
