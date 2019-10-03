# Bob
Bob is distributing storage system designed for byte data like photos. It is has decentralized architecture where each node can handleuser calls. [Pearl](https://github.com/qoollo/pearl) uses like backend storage.

# Api
Bob use grpc [Api](https://github.com/qoollo/bob/blob/master/proto/bob.proto).
Dont type any fields for PutOptions and GetOptions structs.

## BobMeta
```
message BlobMeta {
	int64 timestamp = 1; 
}
```
Timestamp used for data versioning and used must set this field.

# Architecture
[cluster.yaml](https://github.com/qoollo/bob/blob/master/cluster.test.yaml) file descibes all nodes location in cluster and each node data-range. All data separetes in vdisk.  You should create cluster with almost large count vdisks (~100..1000) depends on count nodes. Destination vdisk determs like this: vdisk_id = data_id mod vdisks_count. Then cluster write data on all nodes that store this vdisk.

Example config with 2 nodes and 3 vdisks:
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

As you see there is no replics info. Data writes directly to the nodes so it has great flexibility. Some nodes with more powerfull  hardware can store more data.
# Backend
Backend store data in 2 groups: "normal" and "alien" folders. "Normal" are vdisk folders described in cluster config . "Alien" is folder for data that cannot be written to its node. It has the same structure that "normal" folder but also has "node name" in folder hierarchy. 
Under vdisk info in folder it has timestemp folder info. 
- disk 1
	- vdisk id 1
...
	- vdisk id N
		- timestamp 1
		...
		- timestamp N
			- blob 1
			…
			- blob N
...
- disk N
	- Alien
		- node name
			- vdisk id 1
				- timestamp 1
					- blob 1

# Examples
You can use [bobc](https://github.com/qoollo/bob/blob/master/src/bin/bobc.rs) and [bobp](https://github.com/qoollo/bob/blob/master/src/bin/bobp.rs) like examples
