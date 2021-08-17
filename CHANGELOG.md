# Changelog
Bob versions changelog


## [Unreleased]
#### Added

- Prometheus metrics exporter ([#251](https://github.com/qoollo/bob/pull/251))
- Rate metrics ([#251](https://github.com/qoollo/bob/pull/251))
- Global exporter is used, different exporters may be load conditionally ([#251](https://github.com/qoollo/bob/pull/251))

#### Changed

#### Fixed

#### Updated


## [2.0.0-alpha.1] - 2021-08-16
#### Added
- Add api method to start disk (#182)
- Rest api for data (#187)
- Add bloom filter buffer size to config (#218)
- Setup build with GitHub actions (#266)
- Add Amazon S3 GetObject and PutObject api (#193)
- Add tool for blob recovery (brt) (#205)
- Add racks support to CCG (#186)
- Add bind ip address to config (#270)
- Add variable key size (#133)
- Support for different key sizes in REST API (#228)

#### Changed
- rename bob-tools, remove redundant versions of workspace deps (#220)
- add DiskEventsLogger error (#230)
- add methods for data to REST API specification (#234)
- Update Rocket to 0.5 (#233)
- Extend bob version (#247)

#### Fixed
- Fix backend storage trait object safety issue (#197)
- Fix dockerfiles (#203)
- Fix connectivity issues in Docker Swarm (use 0.0.0.0 in some cases) (#270)
- Fix metrics gaps in graphite (#274)


## [1.6.1] - 2021-04-14
#### Updated
- upgrade pearl to v0.5.14


## [1.6.0] - 2021-03-08
#### Changed
- Reorganisation (#175)
  - split crate into separate subcrates bob, bob-backend, bob-tools
  - move configs, metrics, data, node mods to bob-common
  - tmp remove mock bob client import
  - remove unused deps
  - ignore generated grpc file
  - move pearl metrics to bob-common
  - move config examples

#### Added
- Disk removal control and recovery (#174)
  - Fix: forget to change state in run method
  - Fix review issues
  - Fix semaphore logic
  - Add run semaphore to limit simultaneously initializing DiskControllers
  - Fix test in dcr


## [1.5.1] - 2021-02-24
#### Changed
- parallel index dumping

#### Added
- new alien directory configuration
- new api endpoint: cluster distribution function

#### Fixed
- Build issue after merging request with updated alien directory configuration
- Fix panic in some cases in push_metrics
- fixed metrics prefix
- minor fix for dump semaphores

#### Updated
- Update pearl to 0.5.8


## [1.5.0] - 2021-01-20
#### Changed
- Complete code review and deep refactoring
- Stability and performance improvements
- Search logic of get rewrited. Which leads to improvement of the get performance: up to 50% in normal case, up to 80% in case with reading from aliens
- reworked put algorithm to increase amount of work to be completed in background
- logs improved
- close open blobs by timeout to reduce memory footprint.
- allow partial completion of PUT (#104)
- optional AIO

#### Added
- retrieve records count via http api method, in partition info
- API method to retrieve local dir structure of vdisk replicas. #109
- ssh to docker cluster runner. #106
- for test utility bobp: add all remaining record tasks to last batch. #108
- error descriptionsi for dcr
- dcr instructions to README.md
- nodes to API
- new metrics drain and counters initialization

#### Fixed
- piling all alien records into one vdisk
- loss of records after restart of the cluster
- problems with reading from alien dirs in some cases
- use unreleased version of rio to fix build on musl
- pearl issues with index files
- outdated usage of the partitions old ID. #107
- indexing in bobp with flag -f
- aliens distribution in some cases

#### Updated
- update pearl
- stable tokio runtime


## [1.4.0] - 2020-03-27
#### Updated
- API add partition delete
- bobp errors detalization


## [1.3.0] - 2020-03-02
#### Updated
- pearl updated, improved check existence performance

#### Fixed
- misc minor fixes


## [1.2.1] - 2020-02-06


## [1.2.0] - 2020-01-28
#### Added
- Config cluster generator
- bob test utility


## [1.1.1] - 2020-01-17
#### Fixed
- bobd cli version

#### Updated
- tonic up to 0.1.0


## [1.1.0] - 2019-12-26


## [1.0.0] - 2019-10-03
#### Changed
- Initial version
