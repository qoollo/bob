use crate::prelude::*;

use crate::{
    core::{BackendStorage, Operation},
    mem_backend::{MemBackend, MemDisk},
};

const VDISKS_COUNT: u32 = 10;

pub(crate) fn new_direct(paths: &[String], vdisks_count: u32) -> MemBackend {
    let disks = paths
        .iter()
        .map(|p| (DiskName::from(p), MemDisk::new_direct(DiskName::from(p), vdisks_count)))
        .collect();
    MemBackend {
        disks,
        foreign_data: MemDisk::new_direct(DiskName::from("foreign"), vdisks_count),
    }
}

#[tokio::test]
async fn test_mem_put_wrong_disk() {
    let backend = new_direct(&["name".to_owned()], VDISKS_COUNT);

    let retval = backend
        .put(
            Operation::new_local(0, DiskPath::new("invalid name".into(), "")),
            BobKey::from(1u64),
            &BobData::new(vec![0].into(), BobMeta::stub()),
        )
        .await;
    assert!(retval.err().unwrap().is_internal())
}

#[tokio::test]
async fn test_mem_put_get() {
    let backend = new_direct(&["name".to_owned()], VDISKS_COUNT);

    backend
        .put(
            Operation::new_local(0, DiskPath::new("name".into(), "")),
            BobKey::from(1u64),
            &BobData::new(vec![1].into(), BobMeta::stub()),
        )
        .await
        .unwrap();
    let retval = backend
        .get(
            Operation::new_local(0, DiskPath::new("name".into(), "")),
            BobKey::from(1u64),
        )
        .await
        .unwrap();
    assert_eq!(retval.into_inner(), vec![1]);
}

#[tokio::test]
async fn test_mem_get_wrong_disk() {
    let backend = new_direct(&["name".to_owned()], VDISKS_COUNT);

    backend
        .put(
            Operation::new_local(0, DiskPath::new("name".into(), "")),
            BobKey::from(1u64),
            &BobData::new(vec![1].into(), BobMeta::stub()),
        )
        .await
        .unwrap();
    let retval = backend
        .get(
            Operation::new_local(0, DiskPath::new("invalid name".into(), "")),
            BobKey::from(1u64),
        )
        .await;
    assert!(retval.err().unwrap().is_internal())
}

#[tokio::test]
async fn test_mem_get_no_data() {
    let backend = new_direct(&["name".to_owned()], VDISKS_COUNT);
    let key = BobKey::from(1u64);

    let retval = backend
        .get(
            Operation::new_local(0, DiskPath::new("name".into(), "")),
            key,
        )
        .await;
    assert!(retval.err().unwrap().is_key_not_found())
}
