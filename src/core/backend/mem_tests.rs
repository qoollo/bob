use super::prelude::*;

use super::mem_backend::MemDisk;

const VDISKS_COUNT: u32 = 10;

pub(crate) fn new_direct(paths: &[String], vdisks_count: u32) -> MemBackend {
    let disks = paths
        .iter()
        .map(|p| (p.clone(), MemDisk::new_direct(p.clone(), vdisks_count)))
        .collect();
    MemBackend {
        disks,
        foreign_data: MemDisk::new_direct("foreign".to_string(), vdisks_count),
    }
}

#[tokio::test]
async fn test_mem_put_wrong_disk() {
    let backend = new_direct(&["name".to_owned()], VDISKS_COUNT);

    let retval = backend
        .put(
            BackendOperation::new_local(0, DiskPath::new("invalid name".to_owned(), "".to_owned())),
            1,
            BobData::new(vec![0], BobMeta::stub()),
        )
        .await;
    assert_eq!(retval.err().unwrap(), Error::Internal)
}

#[tokio::test]
async fn test_mem_put_get() {
    let backend = new_direct(&["name".to_owned()], VDISKS_COUNT);

    backend
        .put(
            BackendOperation::new_local(0, DiskPath::new("name".to_owned(), "".to_owned())),
            1,
            BobData::new(vec![1], BobMeta::stub()),
        )
        .await
        .unwrap();
    let retval = backend
        .get(
            BackendOperation::new_local(0, DiskPath::new("name".to_owned(), "".to_owned())),
            1,
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
            BackendOperation::new_local(0, DiskPath::new("name".to_owned(), "".to_owned())),
            1,
            BobData::new(vec![1], BobMeta::stub()),
        )
        .await
        .unwrap();
    let retval = backend
        .get(
            BackendOperation::new_local(0, DiskPath::new("invalid name".to_owned(), "".to_owned())),
            1,
        )
        .await;
    assert_eq!(retval.err().unwrap(), Error::Internal)
}

#[tokio::test]
async fn test_mem_get_no_data() {
    let backend = new_direct(&["name".to_owned()], VDISKS_COUNT);
    let key = 1;

    let retval = backend
        .get(
            BackendOperation::new_local(0, DiskPath::new("name".to_owned(), "".to_owned())),
            key,
        )
        .await;
    assert_eq!(retval.err().unwrap(), Error::KeyNotFound(key))
}
