#[cfg(test)]
mod tests {
    extern crate serde_yaml;
    use bob::core::config::*;

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
        let d: Cluster = parse_config(&s.to_string()).unwrap();
        assert_eq!(false, d.validate());
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
        let d: Cluster = parse_config(&s.to_string()).unwrap();
        assert_eq!(false, d.validate());
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
        let d: Cluster = parse_config(&s.to_string()).unwrap();
        assert_eq!(true, d.validate());
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
        let d: Cluster = parse_config(&s.to_string()).unwrap();
        assert_eq!(false, d.validate());
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
        let d: Cluster = parse_config(&s.to_string()).unwrap();
        assert_eq!(false, d.validate());
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
        let d: Cluster = parse_config(&s.to_string()).unwrap();
        assert_eq!(false, d.validate());
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
        let d: Cluster = parse_config(&s.to_string()).unwrap();
        assert_eq!(false, d.validate());
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
        let d: Cluster = parse_config(&s.to_string()).unwrap();
        assert_eq!(false, d.validate());
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
        let d: Cluster = parse_config(&s.to_string()).unwrap();
        assert_eq!(false, d.validate());
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
        let d: Cluster = parse_config(&s.to_string()).unwrap();
        assert_eq!(false, d.validate());
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
        let d: Cluster = parse_config(&s.to_string()).unwrap();
        assert_eq!(false, d.validate());
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
        let d: Option<Cluster> = parse_config(&s.to_string());
        assert_eq!(true, d.is_none());
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
        let d: Cluster = parse_config(&s.to_string()).unwrap();
        assert_eq!(1, d.nodes.len());
        assert_eq!(1, d.nodes[0].disks.len());
        assert_eq!(1, d.vdisks.len());
        assert_eq!(1, d.vdisks[0].replicas.len());

        assert_eq!(true, d.validate());
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
        let d: Option<Cluster> = parse_config(&s.to_string());
        assert_eq!(true, d.is_none());
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
        let d: Option<Cluster> = parse_config(&s.to_string());
        assert_eq!(true, d.is_none());
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
        let d: Cluster = parse_config(&s.to_string()).unwrap();
        assert_eq!(false, d.validate());
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
        let d: Cluster = parse_config(&s.to_string()).unwrap();
        assert_eq!(false, d.validate());
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
        let d: Cluster = parse_config(&s.to_string()).unwrap();
        assert_eq!(true, d.validate());
        
        let vdisks = conver_to_data(&d);
        assert_eq!(2, vdisks.len());
        assert_eq!(0, vdisks[0].id);
        assert_eq!(1, vdisks[0].replicas.len());
        assert_eq!("/tmp/d1", vdisks[0].replicas[0].path);
        assert_eq!(111, vdisks[0].replicas[0].node.port);


        assert_eq!(1, vdisks[1].id);
        assert_eq!(2, vdisks[1].replicas.len());
        assert_eq!("/tmp/d2", vdisks[1].replicas[0].path);
        assert_eq!(111, vdisks[1].replicas[0].node.port);
        assert_eq!(1111, vdisks[1].replicas[1].node.port);
    }
}