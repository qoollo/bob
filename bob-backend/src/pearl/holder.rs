use std::sync::atomic::{AtomicU64, Ordering};

use crate::{pearl::utils::get_current_timestamp, prelude::*};

use super::{
    core::{BackendResult, PearlStorage},
    data::Key,
    utils::Utils,
};
use bob_common::metrics::pearl::{
    PEARL_GET_BYTES_COUNTER, PEARL_GET_COUNTER, PEARL_GET_ERROR_COUNTER, PEARL_GET_TIMER,
    PEARL_PUT_BYTES_COUNTER, PEARL_PUT_COUNTER, PEARL_PUT_ERROR_COUNTER, PEARL_PUT_TIMER,
    PEARL_DELETE_COUNTER, PEARL_DELETE_ERROR_COUNTER, PEARL_DELETE_TIMER, 
    PEARL_EXIST_COUNTER, PEARL_EXIST_ERROR_COUNTER, PEARL_EXIST_TIMER,
};
use pearl::error::{AsPearlError, ValidationErrorKind};
use pearl::{BlobRecordTimestamp, ReadResult, BloomProvider, FilterResult};

const MAX_TIME_SINCE_LAST_WRITE_SEC: u64 = 10;
const SMALL_RECORDS_COUNT_MUL: u64 = 10;

/// Struct hold pearl and add put/get/restart api
#[derive(Clone, Debug)]
pub struct Holder {
    start_timestamp: u64,
    end_timestamp: u64,
    vdisk: VDiskId,
    disk_path: PathBuf,
    config: PearlConfig,
    storage: Arc<RwLock<PearlSync>>,
    last_modification: Arc<AtomicU64>,
    dump_sem: Arc<Semaphore>,
}

impl Holder {
    pub fn new(
        start_timestamp: u64,
        end_timestamp: u64,
        vdisk: VDiskId,
        disk_path: PathBuf,
        config: PearlConfig,
        dump_sem: Arc<Semaphore>,
    ) -> Self {
        Self {
            start_timestamp,
            end_timestamp,
            vdisk,
            disk_path,
            config,
            storage: Arc::new(RwLock::new(PearlSync::default())),
            last_modification: Arc::new(AtomicU64::new(0)),
            dump_sem,
        }
    }

    pub fn start_timestamp(&self) -> u64 {
        self.start_timestamp
    }

    pub fn end_timestamp(&self) -> u64 {
        self.end_timestamp
    }

    pub fn get_id(&self) -> String {
        self.disk_path
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .unwrap_or("unparsable string")
            .to_owned()
    }

    pub fn storage(&self) -> &RwLock<PearlSync> {
        &self.storage
    }

    pub fn cloned_storage(&self) -> Arc<RwLock<PearlSync>> {
        self.storage.clone()
    }

    pub async fn blobs_count(&self) -> usize {
        let storage = self.storage.read().await;
        storage.blobs_count().await
    }

    pub async fn corrupted_blobs_count(&self) -> usize {
        let storage = self.storage.read().await;
        storage.corrupted_blobs_count()
    }

    pub async fn active_index_memory(&self) -> usize {
        let storage = self.storage.read().await;
        storage.active_index_memory().await
    }

    pub async fn index_memory(&self) -> usize {
        let storage = self.storage.read().await;
        storage.index_memory().await
    }

    pub async fn has_excess_resources(&self) -> bool {
        let storage = self.storage.read().await;
        storage.inactive_index_memory().await > 0
    }

    pub async fn records_count(&self) -> usize {
        let storage = self.storage.read().await;
        storage.records_count().await
    }

    pub fn gets_into_interval(&self, timestamp: u64) -> bool {
        self.start_timestamp <= timestamp && timestamp < self.end_timestamp
    }

    pub fn is_outdated(&self) -> bool {
        let ts = Self::get_current_ts();
        ts > self.end_timestamp
    }

    pub fn is_older_than(&self, secs: u64) -> bool {
        let ts = Self::get_current_ts();
        (ts - secs) > self.end_timestamp
    }

    pub async fn no_modifications_recently(&self) -> bool {
        let ts = Self::get_current_ts();
        let last_modification = self.last_modification();
        ts - last_modification > MAX_TIME_SINCE_LAST_WRITE_SEC
    }

    pub fn last_modification(&self) -> u64 {
        self.last_modification.load(Ordering::Acquire)
    }

    fn update_last_modification(&self) {
        self.last_modification
            .store(Self::get_current_ts(), Ordering::Release);
    }

    pub async fn has_active_blob(&self) -> bool {
        self.storage().read().await.has_active_blob().await
    }

    pub async fn active_blob_is_empty(&self) -> Option<bool> {
        self.storage()
            .read()
            .await
            .active_blob_records_count()
            .await
            .map(|c| c == 0)
    }

    pub async fn active_blob_is_small(&self) -> Option<bool> {
        self.storage()
            .read()
            .await
            .active_blob_records_count()
            .await
            .map(|c| c as u64 * SMALL_RECORDS_COUNT_MUL < self.config.max_data_in_blob())
    }

    fn get_current_ts() -> u64 {
        coarsetime::Clock::now_since_epoch().as_secs()
    }

    pub async fn close_active_blob(&self) {
        // NOTE: during active blob dump (no matter sync or async close) Pearl (~Holder) storage is
        // partly blocked in the same way, the only difference is:
        // 1 [sync case]. Operations will be done one by one, so only one holder would be blocked
        //   at every moment (cleaner will work longer + if there would be a query for not existing
        //   records (so all holders should be checked) bob will be able to fetch records only
        //   between one by one active blob dump queue, because at every moment one holder will be
        //   blocked)
        // 2 [async case]. Operations will be done concurrently, so more holders would be blocked
        //   at every moment, but the whole operation will be performed faster (but remember about
        //   disk_sem and other things, which may slow down this concurrent dump)
        let storage = self.storage.write().await;
        storage.storage().close_active_blob_in_background().await;
        warn!("Active blob of {} closed", self.get_id());
    }

    pub async fn free_excess_resources(&self) -> usize {
        let storage = self.storage.read().await;
        storage.storage().free_excess_resources().await
    }

    pub async fn filter_memory_allocated(&self) -> usize {
        self.storage.read().await.filter_memory_allocated().await
    }

    pub async fn update(&self, storage: Storage<Key>) {
        let mut st = self.storage.write().await;
        st.set(storage.clone());
        st.ready(); // current pearl disk is ready
        debug!(
            "update Pearl id: {}, mark as ready, state: {:?}",
            self.vdisk, st
        );
    }

    pub async fn write(&self, key: BobKey, data: &BobData) -> BackendResult<()> {
        let state = self.storage.read().await;

        if state.is_ready() {
            let storage = state.get();
            self.update_last_modification();
            trace!("Vdisk: {}, write key: {}", self.vdisk, key);
            Self::write_disk(storage, Key::from(key), data).await
        } else {
            trace!("Vdisk: {} isn't ready for writing: {:?}", self.vdisk, state);
            Err(Error::vdisk_is_not_ready())
        }
    }

    // NOTE: stack + heap size (in fact that's serialized size)
    // NOTE: can be calculated like `Data::from(data).len()`, but that's less efficient
    fn calc_data_size(data: &BobData) -> u64 {
        (std::mem::size_of::<BobData>() + std::mem::size_of_val(data.inner())) as u64
    }

    // @TODO remove redundant return result
    #[allow(clippy::cast_possible_truncation)]
    async fn write_disk(storage: PearlStorage, key: Key, data: &BobData) -> BackendResult<()> {
        counter!(PEARL_PUT_COUNTER, 1);
        let data_size = Self::calc_data_size(&data);
        let timer = Instant::now();
        let res = storage.write(key, data.to_serialized_bytes()).await;
        let res = match res {
            Err(e) => {
                counter!(PEARL_PUT_ERROR_COUNTER, 1);
                error!("error on write: {:?}", e);
                // on pearl level before write in storage it performs `contain` check which
                // may fail with OS error (that also means that disk is possibly disconnected)
                let new_e = e.as_pearl_error().map_or(
                    Error::possible_disk_disconnection(),
                    |err| match err.kind() {
                        PearlErrorKind::WorkDirUnavailable { .. } => {
                            Error::possible_disk_disconnection()
                        }
                        _ => Error::internal(),
                    },
                );
                //TODO check duplicate
                Err(new_e)
            }
            Ok(()) => {
                counter!(PEARL_PUT_BYTES_COUNTER, data_size);
                Ok(())
            }
        };
        counter!(PEARL_PUT_TIMER, timer.elapsed().as_nanos() as u64);
        res
    }

    #[allow(clippy::cast_possible_truncation)]
    pub async fn read(&self, key: BobKey) -> Result<ReadResult<BobData>, Error> {
        let state = self.storage.read().await;
        if state.is_ready() {
            let storage = state.get();
            trace!("Vdisk: {}, read key: {}", self.vdisk, key);
            counter!(PEARL_GET_COUNTER, 1);
            let timer = Instant::now();
            let res = storage
                .read(Key::from(key))
                .await
                .map_err(|e| {
                    counter!(PEARL_GET_ERROR_COUNTER, 1);
                    trace!("error on read: {:?}", e);
                    Error::storage(e.to_string())
                })
                .and_then(|r| match r {
                    ReadResult::Found(v) => {
                        counter!(PEARL_GET_BYTES_COUNTER, v.len() as u64);
                        BobData::from_serialized_bytes(v).map(|d| ReadResult::Found(d))
                    }
                    ReadResult::Deleted(ts) => Ok(ReadResult::Deleted(ts)),
                    ReadResult::NotFound => {
                        counter!(PEARL_GET_ERROR_COUNTER, 1);
                        Ok(ReadResult::NotFound)
                    }
                });
            counter!(PEARL_GET_TIMER, timer.elapsed().as_nanos() as u64);
            res
        } else {
            trace!("Vdisk: {} isn't ready for reading: {:?}", self.vdisk, state);
            Err(Error::vdisk_is_not_ready())
        }
    }

    pub async fn try_reinit(&self) -> BackendResult<()> {
        let mut state = self.storage.write().await;
        if state.is_reinit() {
            trace!(
                "Vdisk: {} reinitializing now, state: {:?}",
                self.vdisk,
                state
            );
            Err(Error::vdisk_is_not_ready())
        } else {
            state.init();
            trace!("Vdisk: {} set as reinit, state: {:?}", self.vdisk, state);
            let storage = state.get();
            trace!("Vdisk: {} close old Pearl", self.vdisk);
            let result = storage.close().await;
            if let Err(e) = result {
                error!("can't close pearl storage: {:?}", e);
                // we can't do anything
            }
            Ok(())
        }
    }

    pub async fn exist(&self, key: BobKey) -> Result<ReadResult<BlobRecordTimestamp>, Error> {
        let state = self.storage.read().await;
        if state.is_ready() {
            trace!("Vdisk: {}, check key: {}", self.vdisk, key);
            counter!(PEARL_EXIST_COUNTER, 1);
            let pearl_key = Key::from(key);
            let storage = state.get();
            let timer = Instant::now();
            let res = storage
                .contains(pearl_key)
                .await
                .map_err(|e| {
                    error!("{:?}", e);
                    counter!(PEARL_EXIST_ERROR_COUNTER, 1);
                    Error::storage(e.to_string())
                });
            counter!(PEARL_EXIST_TIMER, timer.elapsed().as_nanos() as u64);
            res
        } else {
            trace!("Vdisk: {} not ready for reading: {:?}", self.vdisk, state);
            Err(Error::vdisk_is_not_ready())
        }
    }

    pub async fn prepare_storage(&self) -> Result<(), Error> {
        debug!("backend pearl holder prepare storage");
        self.config
            .try_multiple_times_async(
                || self.init_holder(),
                "can't initialize holder",
                self.config.fail_retry_timeout(),
            )
            .await
            .map_err(|e| {
                let storage_error = Error::storage("Failed to init holder");
                if let Some(err) = e.as_pearl_error() {
                    if let PearlErrorKind::Validation { kind, cause: _ } = err.kind() {
                        if matches!(kind, ValidationErrorKind::BlobVersion) {
                            panic!("unsupported pearl blob file version: {:#}", err);
                        }
                    }
                }
                e.downcast_ref::<IOError>().map_or(
                    e.downcast_ref::<Error>()
                        .cloned()
                        .unwrap_or_else(|| storage_error.clone()),
                    |os_error| match os_error.kind() {
                        IOErrorKind::Other | IOErrorKind::PermissionDenied => {
                            Error::possible_disk_disconnection()
                        }
                        _ => storage_error,
                    },
                )
            })
    }

    async fn init_holder(&self) -> AnyResult<()> {
        let f = || Utils::check_or_create_directory(&self.disk_path);
        self.config
            .try_multiple_times_async(
                f,
                &format!("cannot check path: {:?}", self.disk_path),
                self.config.fail_retry_timeout(),
            )
            .await?;

        self.config
            .try_multiple_times_async(
                || Utils::drop_pearl_lock_file(&self.disk_path),
                &format!("cannot delete lock file: {:?}", self.disk_path),
                self.config.fail_retry_timeout(),
            )
            .await?;

        let storage = self
            .config
            .try_multiple_times(
                || self.init_pearl_by_path(),
                &format!("can't init pearl by path: {:?}", self.disk_path),
                self.config.fail_retry_timeout(),
            )
            .await
            .with_context(|| "backend pearl holder init storage failed")?;
        self.init_pearl(storage).await?;
        debug!("backend pearl holder init holder ready #{}", self.vdisk);
        Ok(())
    }

    async fn init_pearl(&self, mut storage: Storage<Key>) -> Result<(), Error> {
        let ts = get_current_timestamp();
        let res = if self.gets_into_interval(ts) {
            storage.init().await
        } else {
            storage.init_lazy().await
        };
        match res {
            Ok(_) => {
                self.update(storage).await;
                Ok(())
            }
            Err(e) => Err(Error::storage(format!("pearl error: {:?}", e))),
        }
    }

    pub async fn drop_directory(&self) -> BackendResult<()> {
        Utils::drop_directory(&self.disk_path).await
    }

    fn init_pearl_by_path(&self) -> AnyResult<PearlStorage> {
        let mut builder = Builder::new().work_dir(&self.disk_path);

        if self.config.allow_duplicates() {
            builder = builder.allow_duplicates();
        }

        // @TODO add default values to be inserted on deserialisation step
        let prefix = self.config.blob_file_name_prefix();
        let max_data = self.config.max_data_in_blob();
        let max_blob_size = self.config.max_blob_size();
        let mut filter_config = BloomConfig::default();
        if let Some(count) = self.config.max_buf_bits_count() {
            filter_config.max_buf_bits_count = count;
            debug!("bloom filter max buffer bits count set to: {}", count);
        }
        let builder = builder
            .blob_file_name_prefix(prefix)
            .max_data_in_blob(max_data)
            .max_blob_size(max_blob_size)
            .set_filter_config(filter_config)
            .set_dump_sem(self.dump_sem.clone());
        let builder = if self.config.is_aio_enabled() {
            match rio::new() {
                Ok(ioring) => {
                    warn!("bob will start with AIO - async fs io api");
                    builder.enable_aio(ioring)
                }
                Err(e) => {
                    warn!("bob will start with standard sync fs io api");
                    warn!("can't start with AIO, cause: {}", e);
                    self.config.set_aio(false);
                    builder
                }
            }
        } else {
            warn!("bob will start with standard sync fs io api");
            warn!("cause: disabled in config");
            builder
        };
        builder
            .build()
            .with_context(|| format!("cannot build pearl by path: {:?}", &self.disk_path))
    }

    pub async fn delete(&self, key: BobKey, _meta: &BobMeta, force_delete: bool) -> Result<u64, Error> {
        let state = self.storage.read().await;
        if state.is_ready() {
            let storage = state.get();
            trace!("Vdisk: {}, delete key: {}", self.vdisk, key);
            counter!(PEARL_DELETE_COUNTER, 1);
            let timer = Instant::now();
            // TODO: use meta
            let res = storage
                .delete(Key::from(key), !force_delete)
                .await
                .map_err(|e| {
                    trace!("error on delete: {:?}", e);
                    counter!(PEARL_DELETE_ERROR_COUNTER, 1);
                    Error::storage(e.to_string())
                });
            self.update_last_modification();
            counter!(PEARL_DELETE_TIMER, timer.elapsed().as_nanos() as u64);
            res
        } else {
            trace!("Vdisk: {} isn't ready for reading: {:?}", self.vdisk, state);
            Err(Error::vdisk_is_not_ready())
        }
    }

    pub async fn close_storage(&self) {
        let lck = self.storage();
        let pearl_sync = lck.write().await;
        let storage = pearl_sync.storage().clone();
        if let Err(e) = storage.fsyncdata().await {
            warn!("pearl fsync error: {:?}", e);
        }
        if let Err(e) = storage.close().await {
            warn!("pearl close error: {:?}", e);
        }
    }

    pub async fn disk_used(&self) -> u64 {
        let storage = self.storage.read().await;
        storage.storage().disk_used().await
    }
}

#[async_trait::async_trait]
impl BloomProvider<Key> for Holder {
    type Filter = <Storage<Key> as BloomProvider<Key>>::Filter;
    async fn check_filter(&self, item: &Key) -> FilterResult {
        let storage = self.storage().read().await;
        if let Some(storage) = &storage.storage {
            return BloomProvider::check_filter(storage, item).await;
        }
        FilterResult::NeedAdditionalCheck
    }

    fn check_filter_fast(&self, _item: &Key) -> FilterResult {
        FilterResult::NeedAdditionalCheck
    }

    async fn offload_buffer(&mut self, needed_memory: usize, level: usize) -> usize {
        let mut storage = self.storage().write().await;
        if let Some(storage) = &mut storage.storage {
            storage.offload_buffer(needed_memory, level).await
        } else {
            0
        }
    }

    async fn get_filter(&self) -> Option<Self::Filter> {
        let storage = self.storage().read().await;
        if let Some(storage) = &storage.storage {
            storage.get_filter().await
        } else {
            None
        }
    }

    fn get_filter_fast(&self) -> Option<&Self::Filter> {
        None
    }

    async fn filter_memory_allocated(&self) -> usize {
        let storage = self.storage().read().await;
        if let Some(storage) = &storage.storage {
            storage.filter_memory_allocated().await
        } else {
            0
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum PearlState {
    // pearl is started and working
    Normal,
    // pearl restarting
    Initializing,
}

#[derive(Clone, Debug)]
pub struct PearlSync {
    storage: Option<PearlStorage>,
    state: PearlState,
    start_time_test: u8,
}
impl PearlSync {
    pub fn storage(&self) -> &PearlStorage {
        self.storage.as_ref().expect("pearl storage")
    }

    pub async fn records_count(&self) -> usize {
        self.storage().records_count().await
    }

    pub async fn active_index_memory(&self) -> usize {
        self.storage().active_index_memory().await
    }

    pub async fn inactive_index_memory(&self) -> usize {
        self.storage().inactive_index_memory().await
    }

    pub async fn index_memory(&self) -> usize {
        self.storage().index_memory().await
    }

    pub async fn active_blob_records_count(&self) -> Option<usize> {
        self.storage().records_count_in_active_blob().await
    }

    pub async fn blobs_count(&self) -> usize {
        self.storage().blobs_count().await
    }

    pub fn corrupted_blobs_count(&self) -> usize {
        self.storage().corrupted_blobs_count()
    }

    pub async fn has_active_blob(&self) -> bool {
        self.storage().has_active_blob().await
    }

    #[inline]
    pub fn ready(&mut self) {
        self.set_state(PearlState::Normal);
    }

    #[inline]
    pub fn init(&mut self) {
        self.set_state(PearlState::Initializing);
    }

    #[inline]
    pub fn is_ready(&self) -> bool {
        self.state == PearlState::Normal
    }

    #[inline]
    pub fn is_reinit(&self) -> bool {
        self.state == PearlState::Initializing
    }

    #[inline]
    pub fn set_state(&mut self, state: PearlState) {
        self.state = state;
    }

    #[inline]
    pub fn set(&mut self, storage: PearlStorage) {
        self.storage = Some(storage);
        self.start_time_test += 1;
    }

    #[inline]
    pub fn get(&self) -> PearlStorage {
        self.storage.clone().expect("cloned storage")
    }

    pub async fn filter_memory_allocated(&self) -> usize {
        if let Some(storage) = &self.storage {
            storage.filter_memory_allocated().await
        } else {
            0
        }
    }

    pub async fn offload_buffer(&mut self, needed_memory: usize, level: usize) -> usize {
        if let Some(storage) = &mut self.storage {
            storage.offload_buffer(needed_memory, level).await
        } else {
            0
        }
    }
}

impl Default for PearlSync {
    fn default() -> Self {
        Self {
            storage: None,
            state: PearlState::Initializing,
            start_time_test: 0,
        }
    }
}
