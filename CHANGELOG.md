# Changelog
Bob versions changelog


## [Unreleased]
#### Added
- Added grpc exist testing feature to bobp


#### Changed
- File descriptors metric now tries to use lsof | wc first (#359)


#### Fixed
- No more use of MockBobClient in production (#389)
- Ubuntu docker image build error (#412)
- Get & Put speed calculation in bobp


#### Updated
- Configs now support human readable formats (in max_blob_size & bloom_filter_memory_limit) (#388)


## [2.0.0-alpha.11] - 2021-12-10
#### Added
- Hierarchical filters (#333)


#### Fixed
- Disk space metrics calculation fix (#376)


#### Updated
- Upgrade pearl to v0.9.0


## [2.0.0-alpha.10] - 2021-12-02
#### Added
- Add bloom_filter_memory_limit to example config (#378)
- Add traits to support pearl #123


#### Updated
- Upgrade pearl to v0.8.1


## [2.0.0-alpha.9] - 2021-11-19
#### Fixed
- Fix allocated size computation in BloomFilterMemoryLimitHooks (#372)


## [2.0.0-alpha.8] - 2021-11-18
#### Added
- Bloom filter offloading (#301)


## [2.0.0-alpha.7] - 2021-11-09
#### Added
- Add brt mode to reverse byte order (#286)
- Add alien blobs sync API (#334)


#### Changed
- Disable AIO by default (#335)


#### Fixed
- Hardware metrics names for RAM and disk space (#358)


#### Updated
- Upgrade pearl to v0.8.0



## [2.0.0-alpha.6] - 2021-10-19
#### Added
- Add hardware metrics (#242)
- Add REST API metrics (#255)
- Include brt into release builds (rpm, deb and zip) (#344)


#### Changed
- Cleaner closes blobs instead of update (#285)
- Only stable releases to latest tag on Docker Hub (#339)
- Make Bob compile on stable toolchain.
- Add blob and index verification to blob recovery tool (#230)


#### Fixed
- Remove prometheus metrics expiration.


#### Updated
- Upgrade pearl to v0.7.1


## [2.0.0-alpha.5] - 2021-10-02
#### Added
- Add random mode in get in bobp (#215)

#### Updated
- upgrade pearl to v0.7.0.


## [2.0.0-alpha.4] - 2021-09-16
#### Added
- Add the ability to choose prometheus exporter address (#311)
- Add rest api port to config (#269)

#### Fixed
- Prometheus exporter bug which occured after migration to global exporter scheme (#322)
- Minor build issues (#327)
- Fix actual holder creation condition(#283)

#### Updated
- Libs: tonic, tonic-build, tower, tokio.
- Add Bob key size to help and version commands output
- Updated --version output format


## [2.0.0-alpha.3] - 2021-08-31
#### Added
- Add the ability to disable metrics (#241)
- Use default config in dockerfiles (#290)
- Add log file rotation to logger.yaml in examples (#297)
- Build docker images with build workflow (#308)
- Build Bob versions with 8-byte and 16-byte keys (#307)

#### Changed
- once aio is failed, it's disabled in config (#257)


## [2.0.0-alpha.2] - 2021-08-23
#### Added
- Prometheus metrics exporter (#240)
- Rate metrics (#242)
- Global exporter is used, different exporters may be load conditionally
- Run tests with GitHub Actions (#279)

#### Fixed
- Add brt to dockerfiles (#296)
- Bug with panic on load operation in rate processor (#302)


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
