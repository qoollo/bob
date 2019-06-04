use crate::core::backend::*;
use crate::core::data::{BobData, BobKey, VDiskId, VDiskMapper};
use futures::future::{err, ok};

pub struct PearlBackend {

}


impl BackendStorage for PearlBackend {
    fn put(&self, disk: String, vdisk: VDiskId, key: BobKey, data: BobData) -> BackendPutFuture {
        unimplemented!();
    }

    fn put_alien(&self, vdisk: VDiskId, key: BobKey, data: BobData) -> BackendPutFuture {
        unimplemented!();
    }

    fn get(&self, disk: String, vdisk: VDiskId, key: BobKey) -> BackendGetFuture {
        unimplemented!();
    }

    fn get_alien(&self, vdisk: VDiskId, key: BobKey) -> BackendGetFuture {
        unimplemented!();
    }
}