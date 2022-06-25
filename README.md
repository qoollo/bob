# Bob
[![build](https://github.com/qoollo/bob/actions/workflows/build.yml/badge.svg)](https://github.com/qoollo/bob/actions/workflows/build.yml) [![Docker Pulls](https://img.shields.io/docker/pulls/qoollo/bob?color=brightgreen)](https://hub.docker.com/r/qoollo/bob/)

![Bob](https://raw.githubusercontent.com/qoollo/bob/305-bob-logo/logo/bob-git-docker-logo.svg?sanitize=true)

Bob is a distributed storage system designed for byte data such as photos. It has decentralized architecture where each node can handle user calls. [Pearl](https://github.com/qoollo/pearl) uses as backend storage.

More information can be found on [wiki](https://github.com/qoollo/bob/wiki).

# Api
Bob uses grpc [Api](https://github.com/qoollo/bob/blob/master/bob-grpc/proto/bob.proto).
This means that you can automatically generate a protocol implementation for any platform. 

Detailed API description can be found on [wiki](https://github.com/qoollo/bob/wiki/gRPC-API).

## BobMeta
```
message BlobMeta {
	int64 timestamp = 1; 
}
```
Timestamp used for data versioning and user must set this field.

# Architecture
[cluster.yaml](https://github.com/qoollo/bob/wiki/Cluster-config) file describes the addresses of all nodes in the cluster and a set of directories/physical disks on each node. All data is logically distributed across virtual disks (vdisks). You should create a cluster with more virtual disks than physical ones (it would be reasonable to place 3..10 vdisks on 1 physical disk). Destination vdisk determs like this: `vdisk_id = data_id % vdisks_count`. Cluster writes data to all nodes including target vdisk.

Example config with 2 nodes and 3 vdisks:
```YAML
- nodes:
  - name: node1
    address: 127.0.0.1:20000
    disks:
      - name: disk1
        path: /tmp/d1
  - name: node2
    address: 127.0.0.1:20001
    disks:
      - name: disk1
        path: /tmp/d1
      - name: disk2
        path: /tmp/d2

- vdisks:
  - id: 0
    replicas:
      - node: node1
        disk: disk1
  - id: 1
    replicas:
      - node: node2
        disk: disk1
  - id: 2
    replicas:
      - node: node1
        disk: disk1
      - node: node2
        disk: disk2
```

As you can see, there are no replicated nodes. Data is written to vdisks that are distributed across nodes, so it has great flexibility. Some nodes with more powerfull hardware can store more data.

`cluster.yaml` can be generated semi-automatically with [CCG](https://github.com/qoollo/bob/wiki/Tools#cluster-config-generator-ccg).

# Backend
Backend store data in 2 groups: "normal" and "alien" folders. "Normal" are vdisk folders described in cluster config . "Alien" is folder for data that cannot be written to its node. It has the same structure that "normal" folder but also has "node name" in folder hierarchy. 
Under vdisk info in folder it has timestamp folder info. 
```
- disk 1
    - vdisk id 1
...
    - vdisk id N
        - timestamp 1
        ...
        - timestamp N
            - blob 1
            ...
            - blob N
...
- disk N
    - Alien
        - node name
            - vdisk id 1
                - timestamp 1
                    - blob 1
```

# Examples
You can use [bobc](https://github.com/qoollo/bob/blob/master/bob-apps/bin/bobc.rs) and [bobp](https://github.com/qoollo/bob/blob/master/bob-apps/bin/bobp.rs) like examples.

Also you can use [dcr](https://github.com/qoollo/bob/blob/master/bob-apps/bin/dcr.rs) to create and start docker-compose configuration. By default dcr takes configuration from file [dcr_config.yaml](https://github.com/qoollo/bob/blob/master/config-examples/dcr_config.yaml). All configuration files are saved in cluster_test directory. SSH can be used to connect to running dockers. Can be used with flag -g to generate configs without starting dockers.

# Performance

Bob can handle more than 10k RPS. 

Detailed tests and hardware configuration can be found on [wiki](https://github.com/qoollo/bob/wiki/Performance-tests).
