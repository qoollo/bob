use super::prelude::*;

use crate::core_inner::backend::pearl::core::PearlBackend;
use crate::core_inner::configs::{node::NodeConfigYaml, ClusterConfigYaml};
use std::fs::remove_dir_all;

static DISK_NAME: &str = "disk1";
static PEARL_PATH: &str = "/tmp/d1/";
const KEY_ID: u64 = 1;
const TIMESTAMP: i64 = 1;

fn drop_pearl() {
    let path = PathBuf::from(PEARL_PATH);
    if path.exists() {
        remove_dir_all(path).unwrap();
    }
}

fn create_backend(node_config: &str, cluster_config: &str) -> PearlBackend {
    let (vdisks, cluster) = ClusterConfigYaml::get_from_string(cluster_config).unwrap();
    debug!("vdisks: {:?}", vdisks);
    debug!("cluster: {:?}", cluster);
    let node = NodeConfigYaml::get_from_string(node_config, &cluster).unwrap();
    debug!("node: {:?}", node);

    let mapper = Arc::new(VDiskMapper::new(vdisks.to_vec(), &node, &cluster));
    debug!("mapper: {:?}", mapper);
    PearlBackend::new(mapper, &node)
}

fn backend() -> PearlBackend {
    let node_config = "
log_config: logger.yaml
name: local_node
quorum: 1
operation_timeout: 3sec
check_interval: 5000ms
cluster_policy: quorum             # quorum
backend_type: pearl                # in_memory, stub, pearl
pearl:                             # used only for 'backend_type: pearl'
  max_blob_size: 10000000          # size in bytes. required for 'pearl'
  max_data_in_blob: 10000          # optional
  blob_file_name_prefix: bob       # optional
  pool_count_threads: 4            # required for 'pearl'
  fail_retry_timeout: 100ms
  alien_disk: disk1                # required for 'pearl'
  settings:                        # describes how create and manage bob directories. required for 'pearl'
    root_dir_name: bob             # root dir for bob storage. required for 'pearl'
    alien_root_dir_name: alien     # root dir for alien storage in 'alien_disk'. required for 'pearl'
    timestamp_period: 1d           # period when new pearl directory created. required for 'pearl'
    create_pearl_wait_delay: 100ms
";
    let cluster_config = "
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
    debug!("node_config: {}", node_config);
    debug!("cluster_config: {}", cluster_config);
    create_backend(node_config, cluster_config)
}

#[tokio::test]
async fn test_write_multiple_read() {
    drop_pearl();
    let vdisk_id = VDiskId::new(0);
    let backend = backend();
    backend.run_backend().await.unwrap();

    let write = backend
        .put(
            BackendOperation::new_local(vdisk_id.clone(), DiskPath::new(DISK_NAME, "")),
            BobKey::new(KEY_ID),
            BobData::new(vec![], BobMeta::new_value(TIMESTAMP)),
        )
        .0
        .await;
    assert!(write.is_ok());

    let mut read = backend
        .get(
            BackendOperation::new_local(vdisk_id.clone(), DiskPath::new(DISK_NAME, "")),
            BobKey::new(KEY_ID),
        )
        .0
        .await;
    assert_eq!(TIMESTAMP, read.unwrap().data.meta.timestamp);
    read = backend
        .get(
            BackendOperation::new_local(vdisk_id.clone(), DiskPath::new(DISK_NAME, "")),
            BobKey::new(KEY_ID),
        )
        .0
        .await;
    assert_eq!(TIMESTAMP, read.unwrap().data.meta.timestamp);

    let result1 = backend
        .get(
            BackendOperation::new_local(vdisk_id.clone(), DiskPath::new(DISK_NAME, "")),
            BobKey::new(KEY_ID),
        )
        .0
        .await;
    let result2 = backend
        .get(
            BackendOperation::new_local(vdisk_id.clone(), DiskPath::new(DISK_NAME, "")),
            BobKey::new(KEY_ID),
        )
        .0
        .await;
    assert_eq!(TIMESTAMP, result1.unwrap().data.meta.timestamp);
    assert_eq!(TIMESTAMP, result2.unwrap().data.meta.timestamp);
    drop_pearl();
}
