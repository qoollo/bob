# Bob
[![build](https://github.com/qoollo/bob/actions/workflows/build.yml/badge.svg)](https://github.com/qoollo/bob/actions/workflows/build.yml) [![Docker Pulls](https://img.shields.io/docker/pulls/qoollo/bob?color=brightgreen)](https://hub.docker.com/r/qoollo/bob/)

![Bob](logo/bob-git-docker-logo.svg)

Bob is a distributed storage system designed for byte data such as photos. It has decentralized architecture where each node can handle user calls. [Pearl](https://github.com/qoollo/pearl) is used as a backend storage.

More information can be found on [wiki](https://github.com/qoollo/bob/wiki).

# Installation
For Linux x86_64, you can use RPM or DEB packages from the [releases](https://github.com/qoollo/bob/releases) page.
There is also a ZIP archive with compiled x64 binaries.

Docker images can be found on DockerHub [qoollo/bob](https://hub.docker.com/r/qoollo/bob/).

Additionally, you can build Bob from sources. Instruction can be found [here](https://github.com/qoollo/bob/wiki/Build-from-source).

# API
Bob uses gRPC API with the following proto file: [bob.proto](bob-grpc/proto/bob.proto).
This means that you can automatically generate a protocol implementation for any platform. 
Detailed gRPC API description can be found on [wiki](https://github.com/qoollo/bob/wiki/gRPC-API).

There is also a rich client for .NET: [https://github.com/qoollo/bob-client-net](https://github.com/qoollo/bob-client-net).

# Tools
You can use [bobc](https://github.com/qoollo/bob/blob/master/bob-apps/bin/bobc.rs) and [bobp](https://github.com/qoollo/bob/wiki/Tools#bob-benchmark-tool-bobp) to access data on a cluster. `brt` can be used to check and recover corrupted BLOBs.

There are also tools for removing old partitions, recovering aliens, expanding cluster and more, that can be found in [bob-tools](https://github.com/qoollo/bob-tools) repository.

# Performance

Bob can handle more than 10k RPS. 
Detailed tests and hardware configuration can be found on [wiki](https://github.com/qoollo/bob/wiki/Performance-tests).


# Architecture
[cluster.yaml](https://github.com/qoollo/bob/wiki/Cluster-config) file describes the addresses of all nodes in the cluster and a set of directories/physical disks on each node. All data is logically distributed across virtual disks (vdisks). You should create a cluster with more virtual disks than physical ones (it would be reasonable to place 3..10 vdisks on 1 physical disk). Destination vdisk determs like this: `vdisk_id = data_id % vdisks_count`. Cluster writes data to all nodes that contain target vdisk.

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

As you can see, there are no replicated nodes, instead Bob replicates vdisks. Vdisks are distributed across nodes, which provides more flexibility. Some nodes with more powerful hardware can have more vdisks and therefore store more data.

`cluster.yaml` can be generated semi-automatically with [CCG](https://github.com/qoollo/bob/wiki/Tools#cluster-config-generator-ccg).

# Backend
Backend store data in 2 groups: "normal" and "alien" folders. "Normal" are vdisk folders described in cluster config . "Alien" is folder for data that cannot be written to its node due to node unavailability. It has the same structure that "normal" folder but also has "node name" in folder hierarchy. 
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
