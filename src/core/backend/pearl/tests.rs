#[cfg(test)]
mod tests {
    use crate::core::backend;
    use crate::core::backend::pearl::core::*;

    use crate::core::backend::core::BackendStorage;
    use crate::core::configs::cluster::ClusterConfigYaml;
    use crate::core::configs::node::NodeConfigYaml;
    use crate::core::data::{BobData, BobKey, BobMeta, VDiskId, VDiskMapper};
    use futures03::executor::{ThreadPool, ThreadPoolBuilder};
    use std::{fs::remove_dir_all, path::PathBuf};

    static DISK_NAME: &'static str = "disk1";
    static PEARL_PATH: &'static str = "/tmp/d1/";
    const KEY_ID: u64 = 1;
    const TIMESTAMP: u32 = 1;

    fn drop_pearl() {
        let path = PathBuf::from(PEARL_PATH);
        if path.exists() {
            let _ = remove_dir_all(path);
        }
    }

    fn get_pool() -> ThreadPool {
        ThreadPoolBuilder::new().pool_size(4).create().unwrap()
    }

    fn create_backend(
        node_config: &str,
        cluster_config: &str,
        pool: ThreadPool,
    ) -> PearlBackend<ThreadPool> {
        let (vdisks, cluster) = ClusterConfigYaml {}
            .get_from_string(cluster_config)
            .unwrap();
        let node = NodeConfigYaml {}
            .get_from_string(node_config, &cluster)
            .unwrap();

        let mapper = VDiskMapper::new(vdisks.to_vec(), &node);
        PearlBackend::new(mapper, &node, pool)
    }

    fn backend() -> PearlBackend<ThreadPool> {
        let s = "
log_level: Trace
name: local_node
quorum: 1
timeout: 3sec
check_interval: 5000ms
cluster_policy: quorum        # quorum
ping_threads_count: 2
backend_type: pearl            # in_memory, stub, pearl
pearl:                        # used only for 'backend_type: pearl'
  max_blob_size: 10000000      # size in bytes. required for 'pearl'
  max_data_in_blob: 10000     # optional
  blob_file_name_prefix: bob  # optional
  pool_count_threads: 4       # required for 'pearl'
  fail_retry_timeout: 100ms
  alien_disk: disk1           # required for 'pearl'
";
        let s1 = "
nodes:
    - name: local_node
      address: 127.0.0.1:20000
      disks:
        - name: disk1
          path: /tmp/d1
vdisks:
    - id: 0
      replicas:
        - node: local_node
          disk: disk1
";
        create_backend(s, s1, get_pool())
    }

    #[test]
    fn test_write_multiple_read() {
        drop_pearl();
        let vdisk_id = VDiskId::new(0);
        let backend = backend();
        let mut reactor = get_pool();
        let _ = reactor.run(backend.run_backend());

        let write = reactor.run(
            backend
                .put(
                    DISK_NAME.clone().to_string(),
                    vdisk_id.clone(),
                    BobKey::new(KEY_ID),
                    BobData::new(vec![], BobMeta::new_value(TIMESTAMP)),
                )
                .0,
        );
        assert!(write.is_ok());

        let mut read = reactor.run(
            backend
                .get(
                    DISK_NAME.clone().to_string(),
                    vdisk_id.clone(),
                    BobKey::new(KEY_ID),
                )
                .0,
        );
        assert_eq!(TIMESTAMP, read.unwrap().data.meta.timestamp);
        read = reactor.run(
            backend
                .get(
                    DISK_NAME.clone().to_string(),
                    vdisk_id.clone(),
                    BobKey::new(KEY_ID),
                )
                .0,
        );
        assert_eq!(TIMESTAMP, read.unwrap().data.meta.timestamp);

        let q = async move {
            let result1 = backend
                .get(
                    DISK_NAME.clone().to_string(),
                    vdisk_id.clone(),
                    BobKey::new(KEY_ID),
                )
                .0
                .await;
            let result2 = backend
                .get(
                    DISK_NAME.clone().to_string(),
                    vdisk_id.clone(),
                    BobKey::new(KEY_ID),
                )
                .0
                .await;
            assert_eq!(TIMESTAMP, result1.unwrap().data.meta.timestamp);
            assert_eq!(TIMESTAMP, result2.unwrap().data.meta.timestamp);
        };
        let _ = reactor.run(q);
        drop_pearl();
    }

    #[test]
    fn test_read_no_data() {
        drop_pearl();
        let vdisk_id = VDiskId::new(0);
        let backend = backend();
        let mut reactor = get_pool();
        let _ = reactor.run(backend.run_backend());

        let timestamp1 = reactor.run(backend.test(
            DISK_NAME.clone().to_string(),
            vdisk_id.clone(),
            |st| st.start_time_test,
        ));

        let read = reactor.run(
            backend
                .get(
                    DISK_NAME.clone().to_string(),
                    vdisk_id.clone(),
                    BobKey::new(KEY_ID),
                )
                .0,
        );
        assert_eq!(backend::Error::KeyNotFound, read.err().unwrap());

        let timestamp2 = reactor.run(backend.test(
            DISK_NAME.clone().to_string(),
            vdisk_id.clone(),
            |st| st.start_time_test,
        ));
        assert_eq!(timestamp1, timestamp2); // check no restart vdisk-pearl
        drop_pearl();
    }

    // #[test]
    // fn test_write_duplicate() {
    //   drop_pearl();
    //   let vdisk_id = VDiskId::new(0);
    //   let backend = backend();
    //   let mut reactor = get_pool();
    //   let _ = reactor.run(backend.run_backend());

    //   let timestamp1 = reactor.run(backend.test(DISK_NAME.clone().to_string(), vdisk_id.clone(), |st| {st.start_time_test} ));

    //   let mut write = reactor.run(backend.put(DISK_NAME.clone().to_string(), vdisk_id.clone(), BobKey::new(KEY_ID), BobData::new(vec![], BobMeta::new_value(TIMESTAMP))).0);
    //   assert!(write.is_ok());

    //   write = reactor.run(backend.put(DISK_NAME.clone().to_string(), vdisk_id.clone(), BobKey::new(KEY_ID), BobData::new(vec![], BobMeta::new_value(TIMESTAMP))).0);
    //   assert_eq!(backend::Error::DuplicateKey, write.err().unwrap());

    //   let read = reactor.run(backend.get(DISK_NAME.clone().to_string(), vdisk_id.clone(), BobKey::new(KEY_ID)).0);
    //   assert_eq!(TIMESTAMP, read.unwrap().data.meta.timestamp);

    //   let timestamp2 = reactor.run(backend.test(DISK_NAME.clone().to_string(), vdisk_id.clone(), |st| {st.start_time_test} ));
    //   assert_eq!(timestamp1, timestamp2); // check no restart vdisk-pearl
    //   drop_pearl();
    // }

    // #[test]
    // fn test_write_restart_read() {
    //   drop_pearl();
    //   let vdisk_id = VDiskId::new(0);
    //   let backend = backend();
    //   let mut reactor = get_pool();
    //   let _ = reactor.run(backend.run_backend());

    //   let write = reactor.run(backend.put(DISK_NAME.clone().to_string(), vdisk_id.clone(), BobKey::new(KEY_ID), BobData::new(vec![], BobMeta::new_value(TIMESTAMP))).0);
    //   assert!(write.is_ok());

    //   let _ = reactor.run(backend.test_vdisk(DISK_NAME.clone().to_string(), vdisk_id.clone(), |st| {
    //     let q = async move {
    //       let result = st.try_reinit().await;
    //       assert!(result.unwrap());
    //       let _ = st.prepare_storage().await;
    //       Ok(())
    //     };
    //     q.boxed()
    //   }));
    //   let read = reactor.run(backend.get(DISK_NAME.clone().to_string(), vdisk_id.clone(), BobKey::new(KEY_ID)).0);
    //   assert_eq!(TIMESTAMP, read.unwrap().data.meta.timestamp);
    // }

    // #[test]
    // fn test_write_key_with_2_timestamps_read_last() {
    //     let vdisk_id = VDiskId::new(0);
    //     let backend = backend();
    //     let mut reactor = get_pool();
    //     let _ = reactor.run(backend.run_backend());

    //     let mut write = reactor.run(backend.put(DISK_NAME.clone().to_string(), vdisk_id.clone(), BobKey::new(KEY_ID), BobData::new(vec![], BobMeta::new_value(TIMESTAMP))).0);
    //     assert!(write.is_ok());
    //     write = reactor.run(backend.put(DISK_NAME.clone().to_string(), vdisk_id.clone(), BobKey::new(KEY_ID), BobData::new(vec![], BobMeta::new_value(TIMESTAMP2))).0);
    //     assert!(write.is_ok());

    //     let read = reactor.run(backend.get(DISK_NAME.clone().to_string(), vdisk_id.clone(), BobKey::new(KEY_ID)).0);
    //     assert_eq!(TIMESTAMP2, read.unwrap().data.meta.timestamp);
    // }
}
