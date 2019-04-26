#[cfg(test)]
mod tests {
    use crate::core::backend::mem_backend::*;
    use crate::core::backend::*;
    use crate::core::data::*;
    use tokio_core::reactor::Core;

    const VDISKS_COUNT: u32 = 10;

    #[test]
    fn test_mem_put_wrong_disk() {
        let backend = MemBackend::new_test(&["name".to_string()], VDISKS_COUNT);
        let mut reactor = Core::new().unwrap();

        let retval = reactor.run(backend.put(
            &BackendOperation::new_local(VDiskId::new(0), DiskPath::new("invalid name", "path")),
            BobKey { key: 1 },
            BobData { data: vec![0] },
        ));
        assert_eq!(retval.err().unwrap(), BackendError::Other)
    }

    #[test]
    fn test_mem_put_get() {
        let backend = MemBackend::new_test(&["name".to_string()], VDISKS_COUNT);
        let mut reactor = Core::new().unwrap();

        reactor
            .run(backend.put(
                &BackendOperation::new_local(VDiskId::new(0), DiskPath::new("name", "path")),
                BobKey { key: 1 },
                BobData { data: vec![1] },
            ))
            .unwrap();
        let retval = reactor
            .run(backend.get(
                &BackendOperation::new_local(VDiskId::new(0), DiskPath::new("name", "path")),
                BobKey { key: 1 },
            ))
            .unwrap();
        assert_eq!(retval.data.data, vec![1]);
    }

    #[test]
    fn test_mem_get_wrong_disk() {
        let backend = MemBackend::new_test(&["name".to_string()], VDISKS_COUNT);
        let mut reactor = Core::new().unwrap();

        reactor
            .run(backend.put(
                &BackendOperation::new_local(VDiskId::new(0), DiskPath::new("name", "path")),
                BobKey { key: 1 },
                BobData { data: vec![1] },
            ))
            .unwrap();
        let retval = reactor.run(backend.get(
            &BackendOperation::new_local(VDiskId::new(0), DiskPath::new("invalid name", "path")),
            BobKey { key: 1 },
        ));
        assert_eq!(retval.err().unwrap(), BackendError::Other)
    }

    #[test]
    fn test_mem_get_no_data() {
        let backend = MemBackend::new_test(&["name".to_string()], VDISKS_COUNT);
        let mut reactor = Core::new().unwrap();

        let retval = reactor.run(backend.get(
            &BackendOperation::new_local(VDiskId::new(0), DiskPath::new("name", "path")),
            BobKey { key: 1 },
        ));
        assert_eq!(retval.err().unwrap(), BackendError::NotFound)
    }
}
