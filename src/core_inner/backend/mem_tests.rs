use super::prelude::*;

use super::mem_backend::MemDisk;
use futures::executor::{ThreadPool, ThreadPoolBuilder};

const VDISKS_COUNT: u32 = 10;

fn get_pool() -> ThreadPool {
    ThreadPoolBuilder::new().pool_size(1).create().unwrap()
}

pub fn new_direct(paths: &[String], vdisks_count: u32) -> MemBackend {
    let b = paths
        .iter()
        .map(|p| (p.clone(), MemDisk::new_direct(p.clone(), vdisks_count)))
        .collect::<HashMap<String, MemDisk>>();
    MemBackend {
        disks: b,
        foreign_data: MemDisk::new_direct("foreign".to_string(), vdisks_count),
    }
}

#[test]
fn test_mem_put_wrong_disk() {
    let backend = new_direct(&["name".to_string()], VDISKS_COUNT);
    let mut reactor = get_pool();

    let retval = reactor.run(
        backend
            .put(
                BackendOperation::new_local(VDiskId::new(0), DiskPath::new("invalid name", "")),
                BobKey { key: 1 },
                BobData {
                    data: vec![0],
                    meta: BobMeta::new_stub(),
                },
            )
            .0,
    );
    assert_eq!(retval.err().unwrap(), Error::Internal)
}

#[test]
fn test_mem_put_get() {
    let backend = new_direct(&["name".to_string()], VDISKS_COUNT);
    let mut reactor = get_pool();

    reactor
        .run(
            backend
                .put(
                    BackendOperation::new_local(VDiskId::new(0), DiskPath::new("name", "")),
                    BobKey { key: 1 },
                    BobData {
                        data: vec![1],
                        meta: BobMeta::new_stub(),
                    },
                )
                .0,
        )
        .unwrap();
    let retval = reactor
        .run(
            backend
                .get(
                    BackendOperation::new_local(VDiskId::new(0), DiskPath::new("name", "")),
                    BobKey { key: 1 },
                )
                .0,
        )
        .unwrap();
    assert_eq!(retval.data.data, vec![1]);
}

#[test]
fn test_mem_get_wrong_disk() {
    let backend = new_direct(&["name".to_string()], VDISKS_COUNT);
    let mut reactor = get_pool();

    reactor
        .run(
            backend
                .put(
                    BackendOperation::new_local(VDiskId::new(0), DiskPath::new("name", "")),
                    BobKey { key: 1 },
                    BobData {
                        data: vec![1],
                        meta: BobMeta::new_stub(),
                    },
                )
                .0,
        )
        .unwrap();
    let retval = reactor.run(
        backend
            .get(
                BackendOperation::new_local(VDiskId::new(0), DiskPath::new("invalid name", "")),
                BobKey { key: 1 },
            )
            .0,
    );
    assert_eq!(retval.err().unwrap(), Error::Internal)
}

#[test]
fn test_mem_get_no_data() {
    let backend = new_direct(&["name".to_string()], VDISKS_COUNT);
    let mut reactor = get_pool();

    let retval = reactor.run(
        backend
            .get(
                BackendOperation::new_local(VDiskId::new(0), DiskPath::new("name", "")),
                BobKey { key: 1 },
            )
            .0,
    );
    assert_eq!(retval.err().unwrap(), Error::KeyNotFound)
}
