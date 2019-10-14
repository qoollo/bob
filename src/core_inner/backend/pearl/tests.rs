use super::prelude::*;

use crate::core_inner::backend::pearl::core::PearlBackend;
use crate::core_inner::configs::{node::NodeConfigYaml, ClusterConfigYaml};
use env_logger::fmt::Color;
use futures::executor::{ThreadPool, ThreadPoolBuilder};
use log::{Level, LevelFilter};
use std::fs::remove_dir_all;
use std::io::Write;

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

fn get_pool() -> ThreadPool {
    ThreadPoolBuilder::new().pool_size(4).create().unwrap()
}

fn create_backend(
    node_config: &str,
    cluster_config: &str,
    pool: ThreadPool,
) -> PearlBackend<ThreadPool> {
    let (vdisks, cluster) = ClusterConfigYaml::get_from_string(cluster_config).unwrap();
    debug!("vdisks: {:?}", vdisks);
    debug!("cluster: {:?}", cluster);
    let node = NodeConfigYaml::get_from_string(node_config, &cluster).unwrap();
    debug!("node: {:?}", node);

    let mapper = Arc::new(VDiskMapper::new(vdisks.to_vec(), &node, &cluster));
    debug!("mapper: {:?}", mapper);
    PearlBackend::new(mapper, &node, pool)
}

fn backend() -> PearlBackend<ThreadPool> {
    let node_config = "
log_config: logger.yaml
name: local_node
quorum: 1
timeout: 3sec
check_interval: 5000ms
cluster_policy: quorum             # quorum
ping_threads_count: 2
grpc_buffer_bound: 100
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
    create_backend(node_config, cluster_config, get_pool())
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

#[allow(unreachable_code, unused_variables)]
#[tokio::test]
async fn test_read_no_data() {
    env_logger::builder()
        .format(|buf, record: &log::Record| {
            let mut style = buf.style();
            let color = match record.level() {
                Level::Error => Color::Red,
                Level::Warn => Color::Yellow,
                Level::Info => Color::Green,
                Level::Debug => Color::Cyan,
                Level::Trace => Color::White,
            };
            style.set_color(color);
            let mut level = record.level().to_string();
            level.make_ascii_lowercase();
            writeln!(
                buf,
                "{:^5}: {}:{:^4} - {}",
                style.value(level),
                record.module_path().unwrap_or(""),
                style.value(record.line().unwrap_or(0)),
                style.value(record.args())
            )
        })
        .filter_level(LevelFilter::Debug)
        .try_init()
        .unwrap_or(());
    debug!("logger initialized");
    drop_pearl();
    debug!("pearl dropped");
    let vdisk_id = VDiskId::new(0);
    debug!("vdisk_id: {:?}", vdisk_id);
    let backend = backend();
    debug!("backend created");

    error!("backend test contains unimplemented code");
    return;
    let timestamp1 = backend
        .test(DISK_NAME.to_string(), vdisk_id.clone(), |st| {
            st.start_time_test
        })
        .await;
    debug!("timestamp1: {:?}", timestamp1);

    let get_res = backend
        .get(
            BackendOperation::new_local(vdisk_id.clone(), DiskPath::new(DISK_NAME, "")),
            BobKey::new(KEY_ID),
        )
        .0
        .await
        .unwrap_err();
    assert_eq!(Error::KeyNotFound, get_res);
    debug!("get_res: {:?}", get_res);

    let timestamp2 = backend
        .test(DISK_NAME.to_string(), vdisk_id.clone(), |st| {
            st.start_time_test
        })
        .await;
    debug!("timestamp2: {:?}", timestamp2);
    assert_eq!(timestamp1, timestamp2); // check no restart vdisk-pearl
    drop_pearl();
    debug!("pearl dropped");
}

// #[test]
// fn test_write_duplicate() {
//     drop_pearl();
//     let vdisk_id = VDiskId::new(0);
//     let backend = backend();
//     let mut reactor = get_pool();
//     reactor.run(backend.run_backend());

//     let timestamp1 = reactor.run(backend.test(
//         DISK_NAME.to_string(),
//         vdisk_id.clone(),
//         |st| st.start_time_test,
//     ));

//     let mut write = reactor.run(
//         backend
//             .put(
//                 BackendOperation::new_local(vdisk_id.clone(), DiskPath::new(DISK_NAME, "")),
//                 BobKey::new(KEY_ID),
//                 BobData::new(vec![], BobMeta::new_value(TIMESTAMP)),
//             )
//             .0,
//     );
//     assert!(write.is_ok());

//     write = reactor.run(
//         backend
//             .put(
//                 BackendOperation::new_local(vdisk_id.clone(), DiskPath::new(DISK_NAME, "")),
//                 BobKey::new(KEY_ID),
//                 BobData::new(vec![], BobMeta::new_value(TIMESTAMP)),
//             )
//             .0,
//     );
//     assert_eq!(backend::Error::DuplicateKey, write.err().unwrap());

//     let read = reactor.run(
//         backend
//             .get(
//                 BackendOperation::new_local(vdisk_id.clone(), DiskPath::new(DISK_NAME, "")),
//                 BobKey::new(KEY_ID),
//             )
//             .0,
//     );
//     assert_eq!(TIMESTAMP, read.unwrap().data.meta.timestamp);

//     let timestamp2 = reactor.run(backend.test(
//         DISK_NAME.to_string(),
//         vdisk_id.clone(),
//         |st| st.start_time_test,
//     ));
//     assert_eq!(timestamp1, timestamp2); // check no restart vdisk-pearl
//     drop_pearl();
// }

// #[test]
// fn test_write_restart_read() {
//     drop_pearl();
//     let vdisk_id = VDiskId::new(0);
//     let backend = backend();
//     let mut reactor = get_pool();
//     reactor.run(backend.run_backend());

//     let write = reactor.run(
//         backend
//             .put(
//                 BackendOperation::new_local(vdisk_id.clone(), DiskPath::new(DISK_NAME, "")),
//                 BobKey::new(KEY_ID),
//                 BobData::new(vec![], BobMeta::new_value(TIMESTAMP)),
//             )
//             .0,
//     );
//     assert!(write.is_ok());

//     reactor.run(backend.test_vdisk(
//         BackendOperation::new_local(vdisk_id.clone(), DiskPath::new(DISK_NAME, "")),
//         |st| {
//             let q = async move {
//                 let result = st.try_reinit().await;
//                 assert!(result.unwrap());
//                 st.prepare_storage().await;
//                 Ok(())
//             };
//             q.boxed()
//         },
//     ));
//     let read = reactor.run(
//         backend
//             .get(
//                 DISK_NAME.to_string(),
//                 vdisk_id.clone(),
//                 BobKey::new(KEY_ID),
//             )
//             .0,
//     );
//     assert_eq!(TIMESTAMP, read.unwrap().data.meta.timestamp);
// }

// #[test]
// fn test_write_key_with_2_timestamps_read_last() {
//     let vdisk_id = VDiskId::new(0);
//     let backend = backend();
//     let mut reactor = get_pool();
//     reactor.run(backend.run_backend());

//     let mut write = reactor.run(
//         backend
//             .put(
//                 BackendOperation::new_local(vdisk_id.clone(), DiskPath::new(DISK_NAME, "")),
//                 BobKey::new(KEY_ID),
//                 BobData::new(vec![], BobMeta::new_value(TIMESTAMP)),
//             )
//             .0,
//     );
//     assert!(write.is_ok());
//     write = reactor.run(
//         backend
//             .put(
//                 BackendOperation::new_local(vdisk_id.clone(), DiskPath::new(DISK_NAME, "")),
//                 BobKey::new(KEY_ID),
//                 BobData::new(vec![], BobMeta::new_value(TIMESTAMP2)),
//             )
//             .0,
//     );
//     assert!(write.is_ok());

//     let read = reactor.run(
//         backend
//             .get(
//                 BackendOperation::new_local(vdisk_id.clone(), DiskPath::new(DISK_NAME, "")),
//                 BobKey::new(KEY_ID),
//             )
//             .0,
//     );
//     assert_eq!(TIMESTAMP2, read.unwrap().data.meta.timestamp);
// }
