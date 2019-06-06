#[cfg(test)]
mod tests {
    use crate::core::configs::cluster::*;
    use crate::core::configs::node::*;
    use crate::core::configs::reader::*;
    use crate::core::data::VDiskId;

    #[test]
    fn test_node_disk_name_is_empty() {
        let s = "
nodes:
    - name: n1
      address: 0.0.0.0:111
      disks:
        - name: disk1
          path: /tmp/d1
    - name: n2
      address: 0.0.0.0:111
      disks:
        - name:
          path: /tmp/d1
vdisks:
    - id: 0
      replicas:
        - node: n1
          disk: disk1
";
        let d: Cluster = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_some());
    }

    #[test]
    fn test_node_disk_name_is_missing() {
        let s = "
nodes:
    - name: n1
      address: 0.0.0.0:111
      disks:
        - name: disk1
          path: /tmp/d1
    - name: n2
      address: 0.0.0.0:111
      disks:
        - path: /tmp/d1
vdisks:
    - id: 0
      replicas:
        - node: n1
          disk: disk1
";
        let d: Cluster = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_some());
    }

    #[test]
    fn test_node_check_duplicate_disk_names() {
        let s = "
nodes:
    - name: n1
      address: 0.0.0.0:111
      disks:
        - name: disk1
          path: /tmp/d1
        - name: disk1
          path: /tmp/d2
vdisks:
    - id: 0
      replicas:
        - node: n1
          disk: disk1
";
        let d: Cluster = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_some());
    }

    #[test]
    fn test_vdisk_check_duplicate_replicas_no_dup() {
        let s = "
nodes:
    - name: n1
      address: 0.0.0.0:111
      disks:
        - name: disk1
          path: /tmp/d1
        - name: disk2
          path: /tmp/d1
vdisks:
    - id: 0
      replicas:
        - node: n1
          disk: disk1
        - node: n1
          disk: disk2
";
        let d: Cluster = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_none());
    }

    #[test]
    fn test_vdisk_check_duplicate_ids() {
        let s = "
nodes:
    - name: n1
      address: 0.0.0.0:111
      disks:
        - name: disk1
          path: /tmp/d1
        - name: disk2
          path: /tmp/d1
vdisks:
    - id: 0
      replicas:
        - node: n1
          disk: disk1
        - node: n1
          disk: disk2
    - id: 0
      replicas:
        - node: n1
          disk: disk1
";
        let d: Cluster = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_some());
    }

    #[test]
    fn test_cluster_check_duplicate_nodes_names() {
        let s = "
nodes:
    - name: n1
      address: 0.0.0.0:111
      disks:
        - name: disk1
          path: /tmp/d1
        - name: disk2
          path: /tmp/d1
    - name: n1
      address: 0.0.0.0:111
      disks:
        - name: disk1
          path: /tmp/d1
vdisks:
    - id: 0
      replicas:
        - node: n1
          disk: disk1
        - node: n1
          disk: disk2
";
        let d: Cluster = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_some());
    }

    #[test]
    fn test_vdisk_check_duplicate_replicas_dup() {
        let s = "
nodes:
    - name: n1
      address: 0.0.0.0:111
      disks:
        - name: disk1
          path: /tmp/d1
        - name: disk2
          path: /tmp/d1
vdisks:
    - id: 0
      replicas:
        - node: n1
          disk: disk1
        - node: n1
          disk: disk1
";
        let d: Cluster = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_some());
    }

    #[test]
    fn test_node_disk_path_is_missing() {
        let s = "
nodes:
    - name: n1
      address: 0.0.0.0:111
      disks:
        - name: disk1
          path: /tmp/d1
    - name: n2
      address: 0.0.0.0:111
      disks:
        - name: 123
vdisks:
    - id: 0
      replicas:
        - node: n1
          disk: disk1
";
        let d: Cluster = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_some());
    }

    #[test]
    fn test_node_disk_path_is_empty() {
        let s = "
nodes:
    - name: n1
      address: 0.0.0.0:111
      disks:
        - name: disk1
          path: /tmp/d1
    - name: n2
      address: 0.0.0.0:111
      disks:
        - name: 123
          path:      # empty
vdisks:
    - id: 0
      replicas:
        - node: n1
          disk: disk1
";
        let d: Cluster = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_some());
    }

    #[test]
    fn test_node_name_is_empty() {
        let s = "
nodes:
    - name: n1
      address: 0.0.0.0:111
      disks:
        - name: disk1
          path: /tmp/d1
    - name:
      address: 0.0.0.0:111
      disks:
        - name: 123
          path: 123
vdisks:
    - id: 0
      replicas:
        - node: n1
          disk: disk1
";
        let d: Cluster = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_some());
    }

    #[test]
    fn test_node_address_is_empty() {
        let s = "
nodes:
    - name: n1
      address: 0.0.0.0:111
      disks:
        - name: disk1
          path: /tmp/d1
    - name: name
      address:     #empty
      disks:
        - name: 123
          path: 123
vdisks:
    - id: 0
      replicas:
        - node: n1
          disk: disk1
";
        let d: Cluster = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_some());
    }

    #[test]
    fn test_vdisk_replica_node_is_empty() {
        let s = "
nodes:
    - name: n1
      address: 0.0.0.0:111
      disks:
        - name: disk1
          path: /tmp/d1
vdisks:
    - id: 0
      replicas:
        - node:
          disk: disk1
";
        let d: Cluster = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_some());
    }

    #[test]
    fn test_vdisk_replica_disk_is_empty() {
        let s = "
nodes:
    - name: n1
      address: 0.0.0.0:111
      disks:
        - name: disk1
          path: /tmp/d1
vdisks:
    - id: 0
      replicas:
        - node: n1
          disk:         # empty
";
        let d: Cluster = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_some());
    }

    #[test]
    fn test_vdisk_id_is_empty() {
        let s = "
nodes:
    - name: n1
      address: 0.0.0.0:111
      disks:
        - name: disk1
          path: /tmp/d1
vdisks:
    - id:
      replicas:
        - node: n1
          disk: disk1        # empty
    - id:      # empty
      replicas:
        - node: n1
          disk: disk1        # empty
";
        let d: Cluster = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_some());
    }

    #[test]
    fn test_count_fields() {
        let s = "
nodes:
    - name: n1
      address: 0.0.0.0:111
      disks:
        - name: disk1
          path: /tmp/d1
vdisks:
    - id: 0
      replicas:
        - node: n1
          disk: disk1
";
        let d: Cluster = YamlBobConfigReader {}.parse(s).unwrap();
        assert_eq!(1, d.nodes.len());
        assert_eq!(1, d.nodes[0].disks.len());
        assert_eq!(1, d.vdisks.len());
        assert_eq!(1, d.vdisks[0].replicas.len());

        assert!(d.validate().is_none());
    }

    #[test]
    fn test_validate_no_nodes() {
        let s = "
vdisks:
    - id: 0
      replicas:
        - node: disk1
          disk: /tmp/d1
";
        let d: Cluster = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_some());
    }

    #[test]
    fn test_validate_no_vdisks() {
        let s = "
nodes:
    - name: n1
      address: 0.0.0.0:111
      disks:
        - name: disk1
          path: /tmp/d1
";
        let d: Cluster = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_some());
    }

    #[test]
    fn test_validate_no_node_with_name() {
        let s = "
nodes:
    - name: other_name
      address: 0.0.0.0:111
      disks:
        - name: disk1
          path: /tmp/d1
vdisks:
    - id: 0
      replicas:
        - node: some_name
          disk: disk1
";
        let d: Cluster = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_some());
    }

    #[test]
    fn test_validate_no_disk_in_node() {
        let s = "
nodes:
    - name: some_name
      address: 0.0.0.0:111
      disks:
        - name: disk1123123123123123
          path: /tmp/d1
vdisks:
    - id: 0
      replicas:
        - node: some_name
          disk: disk1
";
        let d: Cluster = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_some());
    }

    #[test]
    fn test_cluster_convertation() {
        let s = "
nodes:
    - name: n1
      address: 0.0.0.0:111
      disks:
        - name: disk1
          path: /tmp/d1
        - name: disk2
          path: /tmp/d2
    - name: n2
      address: 0.0.0.0:1111
      disks:
        - name: disk1
          path: /tmp/d3
vdisks:
    - id: 0
      replicas:
        - node: n1
          disk: disk1
    - id: 1
      replicas:
        - node: n1
          disk: disk2
        - node: n2
          disk: disk1
";
        let d: Cluster = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_none());

        let vdisks = ClusterConfigYaml {}.convert_to_data(&d).unwrap();
        assert_eq!(2, vdisks.len());
        assert_eq!(VDiskId::new(0), vdisks[0].id);
        assert_eq!(1, vdisks[0].replicas.len());
        assert_eq!("/tmp/d1", vdisks[0].replicas[0].path);
        assert_eq!(111, vdisks[0].replicas[0].node.port);

        assert_eq!(VDiskId::new(1), vdisks[1].id);
        assert_eq!(2, vdisks[1].replicas.len());
        assert_eq!("/tmp/d2", vdisks[1].replicas[0].path);
        assert_eq!(111, vdisks[1].replicas[0].node.port);
        assert_eq!(1111, vdisks[1].replicas[1].node.port);
    }

    #[test]
    fn test_ip_parsing() {
        let s = "
nodes:
    - name: n1
      address: 0.0.0.0:111
      disks:
        - name: disk1
          path: /tmp/d1
        - name: disk2
          path: /tmp/d2
vdisks:
    - id: 0
      replicas:
        - node: n1
          disk: disk1
";
        let d: Cluster = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_none());

        assert_eq!(111, d.nodes[0].port.get());
        assert_eq!("0.0.0.0", d.nodes[0].host.borrow().to_string());
    }

    #[test]
    fn test_ip_parsing2() {
        let s = "
nodes:
    - name: n1
      address: 0.0.0:111
      disks:
        - name: disk1
          path: /tmp/d1
        - name: disk2
          path: /tmp/d2
vdisks:
    - id: 0
      replicas:
        - node: n1
          disk: disk1
";
        let d: Cluster = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_some());
    }

    #[test]
    fn test_ip_parsing3() {
        let s = "
nodes:
    - name: n1
      address: 0.0.0.0:11111111
      disks:
        - name: disk1
          path: /tmp/d1
        - name: disk2
          path: /tmp/d2
vdisks:
    - id: 0
      replicas:
        - node: n1
          disk: disk1
";
        let d: Cluster = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_some());
    }

    #[test]
    fn test_node_config() {
        let s = "
log_level: Debug
name: no
quorum: 1
timeout: 12h 5min 2ns
check_interval: 100ms
cluster_policy: quorum # quorum
backend_type: stub
";
        let d: NodeConfig = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_none());
    }

    #[test]
    fn test_node_pearl_config_no_pearl_config() {
        let s = "
log_level: Debug
name: no
quorum: 1
timeout: 12h 5min 2ns
check_interval: 100ms
cluster_policy: quorum # quorum
backend_type: pearl
";
        let d: NodeConfig = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_some());
    }

    #[test]
    fn test_node_pearl_config() {
        let s = "
log_level: Debug
name: no
quorum: 1
timeout: 12h 5min 2ns
check_interval: 100ms
cluster_policy: quorum # quorum
backend_type: pearl
pearl:
  max_blob_size: 1
  max_data_in_blob: 1
  blob_file_name_prefix: bob
  pool_count_threads: 4
  alien_disk: disk1
";
        let d: NodeConfig = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_none());
    }

    #[test]
    fn test_node_pearl_config2() {
        let s = "
log_level: Debug
name: no
quorum: 1
timeout: 12h 5min 2ns
check_interval: 100ms
cluster_policy: quorum # quorum
backend_type: pearl
pearl:
  max_blob_size: 1
#  max_data_in_blob: 1
#  blob_file_name_prefix: bob
  pool_count_threads: 4
  alien_disk: disk1  
";
        let d: NodeConfig = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_none());
    }

    #[test]
    fn test_node_pearl_config_no_field() {
        let s = "
log_level: Debug
name: no
quorum: 1
timeout: 12h 5min 2ns
check_interval: 100ms
cluster_policy: quorum # quorum
backend_type: pearl
pearl:
#  max_blob_size: 1
  max_data_in_blob: 1
#  blob_file_name_prefix: bob
  alien_disk: disk1
";
        let d: NodeConfig = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_some());
    }

    #[test]
    fn test_node_config_invalid_backend_type() {
        let s = "
log_level: Debug
name: n1
quorum: 1
timeout: 12h 5min 2ns
check_interval: 100sec
cluster_policy: quorum # quorum
backend_type: InvalidType
";
        let d: NodeConfig = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_none());
        let s1 = "
nodes:
    - name: n1
      address: 0.0.0.0:11111111
      disks:
        - name: disk1
          path: /tmp/d1
        - name: disk2
          path: /tmp/d2
vdisks:
    - id: 0
      replicas:
        - node: n1
          disk: disk1
";
        let cl: Cluster = YamlBobConfigReader {}.parse(s1).unwrap();
        assert!(NodeConfigYaml {}.check_cluster(&cl, &d).is_err());
    }

    #[test]
    fn test_node_config_invalid_time() {
        let s = "
log_level: Debug
name: no
quorum: 1
timeout: 12h 5min 2ns
check_interval: 100mms
cluster_policy: quorum # quorum
backend_type: stub
";
        let d: NodeConfig = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_some());
    }

    #[test]
    fn test_node_config_valid() {
        let s = "
log_level: Debug
name: n1
quorum: 1
timeout: 12h 5min 2ns
check_interval: 100sec
cluster_policy: quorum # quorum
backend_type: stub
";
        let d: NodeConfig = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_none());
        let s1 = "
nodes:
    - name: n1
      address: 0.0.0.0:11111111
      disks:
        - name: disk1
          path: /tmp/d1
        - name: disk2
          path: /tmp/d2
vdisks:
    - id: 0
      replicas:
        - node: n1
          disk: disk1
";
        let cl: Cluster = YamlBobConfigReader {}.parse(s1).unwrap();
        assert!(NodeConfigYaml {}.check_cluster(&cl, &d).is_ok());
    }

    #[test]
    fn test_node_config_invalid() {
        let s = "
log_level: Debug
name: 1n2112321321321321
quorum: 1
timeout: 12h 5min 2ns
check_interval: 100sec
cluster_policy: quorum # quorum
backend_type: stub
";
        let d: NodeConfig = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_none());
        let s1 = "
nodes:
    - name: n1
      address: 0.0.0.0:11111111
      disks:
        - name: disk1
          path: /tmp/d1
        - name: disk2
          path: /tmp/d2
vdisks:
    - id: 0
      replicas:
        - node: n1
          disk: disk1
";
        let cl: Cluster = YamlBobConfigReader {}.parse(s1).unwrap();
        assert!(NodeConfigYaml {}.check_cluster(&cl, &d).is_err());
    }

    #[test]
    fn test_node_config_check_valid_pearl_disk() {
        let s = "
log_level: Debug
name: n1
quorum: 1
timeout: 12h 5min 2ns
check_interval: 100sec
cluster_policy: quorum # quorum
backend_type: pearl
pearl:
#  max_blob_size: 1
  max_data_in_blob: 1
#  blob_file_name_prefix: bob
  alien_disk: disk1
";
        let d: NodeConfig = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_some());
        let s1 = "
nodes:
    - name: n1
      address: 0.0.0.0:11111111
      disks:
        - name: disk1
          path: /tmp/d1
        - name: disk2
          path: /tmp/d2
vdisks:
    - id: 0
      replicas:
        - node: n1
          disk: disk1
";
        let cl: Cluster = YamlBobConfigReader {}.parse(s1).unwrap();
        assert!(NodeConfigYaml {}.check_cluster(&cl, &d).is_ok());
    }

    #[test]
    fn test_node_config_check_invalid_pearl_disk() {
        let s = "
log_level: Debug
name: n1
quorum: 1
timeout: 12h 5min 2ns
check_interval: 100sec
cluster_policy: quorum # quorum
backend_type: pearl
pearl:
#  max_blob_size: 1
  max_data_in_blob: 1
#  blob_file_name_prefix: bob
  alien_disk: disk112312312312321
";
        let d: NodeConfig = YamlBobConfigReader {}.parse(s).unwrap();
        assert!(d.validate().is_some());
        let s1 = "
nodes:
    - name: n1
      address: 0.0.0.0:11111111
      disks:
        - name: disk1
          path: /tmp/d1
        - name: disk2
          path: /tmp/d2
vdisks:
    - id: 0
      replicas:
        - node: n1
          disk: disk1
";
        let cl: Cluster = YamlBobConfigReader {}.parse(s1).unwrap();
        assert!(NodeConfigYaml {}.check_cluster(&cl, &d).is_err());
    }
}
