#[cfg(test)]
mod tests {
    use crate::core::backend::mem_backend::*;
    use crate::core::backend::backend::*;
    use crate::core::data::*;
    use tokio_core::reactor::Core;

    const VDISKS_COUNT: u32 = 10;

    #[test]
    fn test_mem_put_wrong_disk() {
        let backend = MemBackend::new_direct(&["name".to_string()], VDISKS_COUNT);
        let mut reactor = Core::new().unwrap();

        let retval = reactor.run(backend.put(
            "invalid name".to_string(),
            VDiskId::new(0),
            BobKey { key: 1 },
            BobData {
                data: vec![0],
                meta: BobMeta::new_stub(),
            },
        ).0);
        assert_eq!(retval.err().unwrap(), BackendError::Other)
    }

    #[test]
    fn test_mem_put_get() {
        let backend = MemBackend::new_direct(&["name".to_string()], VDISKS_COUNT);
        let mut reactor = Core::new().unwrap();

        reactor
            .run(backend.put(
                "name".to_string(),
                VDiskId::new(0),
                BobKey { key: 1 },
                BobData {
                    data: vec![1],
                    meta: BobMeta::new_stub(),
                },
            ).0)
            .unwrap();
        let retval = reactor
            .run(backend.get("name".to_string(), VDiskId::new(0), BobKey { key: 1 }).0)
            .unwrap();
        assert_eq!(retval.data.data, vec![1]);
    }

    #[test]
    fn test_mem_get_wrong_disk() {
        let backend = MemBackend::new_direct(&["name".to_string()], VDISKS_COUNT);
        let mut reactor = Core::new().unwrap();

        reactor
            .run(backend.put(
                "name".to_string(),
                VDiskId::new(0),
                BobKey { key: 1 },
                BobData {
                    data: vec![1],
                    meta: BobMeta::new_stub(),
                },
            ).0)
            .unwrap();
        let retval = reactor.run(backend.get(
            "invalid name".to_string(),
            VDiskId::new(0),
            BobKey { key: 1 },
        ).0);
        assert_eq!(retval.err().unwrap(), BackendError::Other)
    }

    #[test]
    fn test_mem_get_no_data() {
        let backend = MemBackend::new_direct(&["name".to_string()], VDISKS_COUNT);
        let mut reactor = Core::new().unwrap();

        let retval =
            reactor.run(backend.get("name".to_string(), VDiskId::new(0), BobKey { key: 1 }).0);
        assert_eq!(retval.err().unwrap(), BackendError::NotFound)
    }
}
