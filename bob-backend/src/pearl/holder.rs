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
    storage: Arc<RwLock<PearlSync>>,
    inner: Arc<HolderInner>
}

/// Inner Holder data moved into HolderInner to reduce clonning overhead
#[derive(Debug)]
struct HolderInner {
    start_timestamp: u64,
    end_timestamp: u64,
    vdisk: VDiskId,
    disk_path: PathBuf,
    config: PearlConfig,
    dump_sem: Arc<Semaphore>,
    last_modification: AtomicU64,
    init_protection: Semaphore
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
            storage: Arc::new(RwLock::new(PearlSync::default())),
            inner: Arc::new(HolderInner {
                start_timestamp,
                end_timestamp,
                vdisk,
                disk_path,
                config,          
                dump_sem,
                last_modification: AtomicU64::new(0),
                init_protection: Semaphore::new(1)
            })
        }
    }

    pub fn start_timestamp(&self) -> u64 {
        self.inner.start_timestamp
    }

    pub fn end_timestamp(&self) -> u64 {
        self.inner.end_timestamp
    }

    pub fn get_id(&self) -> String {
        self.inner.disk_path
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .unwrap_or("unparsable string")
            .to_owned()
    }

    pub fn cloned_storage(&self) -> Arc<RwLock<PearlSync>> {
        self.storage.clone()
    }

    pub async fn blobs_count(&self) -> usize {
        let storage = self.storage.read().await;
        if let Some(storage) = storage.get() {
            storage.blobs_count().await
        } else {
            0
        }
    }

    pub async fn corrupted_blobs_count(&self) -> usize {
        let storage = self.storage.read().await;
        if let Some(storage) = storage.get() {
            storage.corrupted_blobs_count()
        } else {
            0
        }
    }

    pub async fn active_index_memory(&self) -> usize {
        let storage = self.storage.read().await;
        if let Some(storage) = storage.get() {
            storage.active_index_memory().await
        } else {
            0
        }
    }

    pub async fn index_memory(&self) -> usize {
        let storage = self.storage.read().await;
        if let Some(storage) = storage.get() {
            storage.index_memory().await
        } else {
            0
        }
    }

    pub async fn has_excess_resources(&self) -> bool {
        let storage = self.storage.read().await;
        if let Some(storage) = storage.get() {
            storage.inactive_index_memory().await > 0
        } else {
            false
        }
    }

    pub async fn records_count(&self) -> usize {
        let storage = self.storage.read().await;
        if let Some(storage) = storage.get() {
            storage.records_count().await
        } else {
            0
        }
    }

    pub fn gets_into_interval(&self, timestamp: u64) -> bool {
        self.inner.start_timestamp <= timestamp && timestamp < self.inner.end_timestamp
    }

    pub fn is_outdated(&self) -> bool {
        let ts = Self::get_current_ts();
        ts > self.inner.end_timestamp
    }

    pub fn is_older_than(&self, secs: u64) -> bool {
        let ts = Self::get_current_ts();
        (ts - secs) > self.inner.end_timestamp
    }

    pub async fn no_modifications_recently(&self) -> bool {
        let ts = Self::get_current_ts();
        let last_modification = self.last_modification();
        ts - last_modification > MAX_TIME_SINCE_LAST_WRITE_SEC
    }

    pub fn last_modification(&self) -> u64 {
        self.inner.last_modification.load(Ordering::Acquire)
    }

    fn update_last_modification(&self) {
        self.inner.last_modification
            .store(Self::get_current_ts(), Ordering::Release);
    }

    pub async fn has_active_blob(&self) -> bool {
        let storage = self.storage.read().await;
        if let Some(storage) = storage.get() {
            storage.has_active_blob().await
        } else {
            false
        }
    }

    pub async fn active_blob_is_empty(&self) -> Option<bool> {
        let storage = self.storage.read().await;
        if let Some(storage) = storage.get() {
            storage.records_count_in_active_blob().await.map(|c| c == 0)
        } else {
            None
        }
    }

    pub async fn active_blob_is_small(&self) -> Option<bool> {
        let storage = self.storage.read().await;
        if let Some(storage) = storage.get() {
            storage.records_count_in_active_blob().await
                .map(|c| c as u64 * SMALL_RECORDS_COUNT_MUL < self.inner.config.max_data_in_blob())
        } else {
            None
        }
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
        if let Some(storage) = storage.get() {
            storage.close_active_blob_in_background().await;
            warn!("Active blob of {} closed", self.get_id());
        }
    }

    pub async fn free_excess_resources(&self) -> usize {
        let storage = self.storage.read().await;
        if let Some(storage) = storage.get() {
            storage.free_excess_resources().await
        } else {
            0
        }
    }

    pub async fn filter_memory_allocated(&self) -> usize {
        let storage = self.storage.read().await;
        if let Some(storage) = storage.get() {
            storage.filter_memory_allocated().await
        } else {
            0
        }
    }

    pub async fn write(&self, key: BobKey, data: &BobData) -> BackendResult<()> {
        let state = self.storage.read().await;

        if let Some(storage) = state.get() {
            self.update_last_modification();
            trace!("Vdisk: {}, write key: {}", self.inner.vdisk, key);
            Self::write_disk(storage, Key::from(key), data).await
        } else {
            trace!("Vdisk: {} isn't ready for writing: {:?}", self.inner.vdisk, state);
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
    async fn write_disk(storage: &PearlStorage, key: Key, data: &BobData) -> BackendResult<()> {
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
        if let Some(storage) = state.get() {
            trace!("Vdisk: {}, read key: {}", self.inner.vdisk, key);
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
            trace!("Vdisk: {} isn't ready for reading: {:?}", self.inner.vdisk, state);
            Err(Error::vdisk_is_not_ready())
        }
    }

    pub async fn exist(&self, key: BobKey) -> Result<ReadResult<BlobRecordTimestamp>, Error> {
        let state = self.storage.read().await;
        if let Some(storage) = state.get() {
            trace!("Vdisk: {}, check key: {}", self.inner.vdisk, key);
            counter!(PEARL_EXIST_COUNTER, 1);
            let pearl_key = Key::from(key);
            let timer = Instant::now();
            let res = storage
                .contains(pearl_key)
                .await
                .map_err(|e| {
                    error!("error on exist: {:?}", e);
                    counter!(PEARL_EXIST_ERROR_COUNTER, 1);
                    Error::storage(e.to_string())
                });
            counter!(PEARL_EXIST_TIMER, timer.elapsed().as_nanos() as u64);
            res
        } else {
            trace!("Vdisk: {} not ready for reading: {:?}", self.inner.vdisk, state);
            Err(Error::vdisk_is_not_ready())
        }
    }

    pub async fn try_reinit(&self) -> BackendResult<()> {
        let _init_protection = self.inner.init_protection.try_acquire().map_err(|err| Error::holder_temporary_unavailable())?;

        let old_storage = {
            let mut state = self.storage.write().await;
            state.reset()
        };

        if let Some(old_storage) = old_storage {
            trace!("Vdisk: {} close old Pearl due to reinit", self.inner.vdisk);
            if let Err(e) = old_storage.close().await {
                error!("can't close pearl storage: {:?}", e);
                // Continue anyway
            }
        }
     
        match self.crate_and_prepare_storage().await {
            Ok(storage) => {
                let mut state = self.storage.write().await;
                state.set_ready(storage).expect("Storage setting successful");
                debug!("update Pearl id: {}, mark as ready, state: ready", self.inner.vdisk);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub async fn prepare_storage(&self) -> Result<(), Error> {
        let _init_protection = self.inner.init_protection.acquire().await.expect("init_protection semaphore acquire error");

        match self.crate_and_prepare_storage().await {
            Ok(storage) => {
                let mut st = self.storage.write().await;
                st.set_ready(storage).expect("Storage setting successful");
                debug!("update Pearl id: {}, mark as ready, state: ready", self.inner.vdisk);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn crate_and_prepare_storage(&self) -> Result<Storage<Key>, Error> {
        debug!("backend pearl holder prepare storage");
        self.inner.config
            .try_multiple_times_async(
                || self.init_holder(),
                "can't initialize holder",
                self.inner.config.fail_retry_timeout(),
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

    async fn init_holder(&self) -> AnyResult<Storage<Key>> {
        let f = || Utils::check_or_create_directory(&self.inner.disk_path);
        self.inner.config
            .try_multiple_times_async(
                f,
                &format!("cannot check path: {:?}", self.inner.disk_path),
                self.inner.config.fail_retry_timeout(),
            )
            .await?;

        self.inner.config
            .try_multiple_times_async(
                || Utils::drop_pearl_lock_file(&self.inner.disk_path),
                &format!("cannot delete lock file: {:?}", self.inner.disk_path),
                self.inner.config.fail_retry_timeout(),
            )
            .await?;

        let mut storage = self
            .inner.config
            .try_multiple_times(
                || self.init_pearl_by_path(),
                &format!("can't init pearl by path: {:?}", self.inner.disk_path),
                self.inner.config.fail_retry_timeout(),
            )
            .await
            .with_context(|| "backend pearl holder init storage failed")?;
        self.init_pearl(&mut storage).await?;
        debug!("backend pearl holder init holder ready #{}", self.inner.vdisk);
        Ok(storage)
    }

    async fn init_pearl(&self, storage: &mut Storage<Key>) -> Result<(), Error> {
        let ts = get_current_timestamp();
        let res = if self.gets_into_interval(ts) {
            storage.init().await
        } else {
            storage.init_lazy().await
        };

        res.map_err(|err| Error::storage(format!("pearl error: {:?}", err)))
    }

    pub async fn drop_directory(&self) -> BackendResult<()> {
        Utils::drop_directory(&self.inner.disk_path).await
    }

    fn init_pearl_by_path(&self) -> AnyResult<PearlStorage> {
        let mut builder = Builder::new().work_dir(&self.inner.disk_path);

        if self.inner.config.allow_duplicates() {
            builder = builder.allow_duplicates();
        }

        // @TODO add default values to be inserted on deserialisation step
        let prefix = self.inner.config.blob_file_name_prefix();
        let max_data = self.inner.config.max_data_in_blob();
        let max_blob_size = self.inner.config.max_blob_size();
        let mut filter_config = BloomConfig::default();
        let validate_data_during_index_regen = self.inner.config.validate_data_checksum_during_index_regen();
        if let Some(count) = self.inner.config.max_buf_bits_count() {
            filter_config.max_buf_bits_count = count;
            debug!("bloom filter max buffer bits count set to: {}", count);
        }
        let builder = builder
            .blob_file_name_prefix(prefix)
            .max_data_in_blob(max_data)
            .max_blob_size(max_blob_size)
            .set_filter_config(filter_config)
            .set_validate_data_during_index_regen(validate_data_during_index_regen)
            .set_dump_sem(self.inner.dump_sem.clone());
        let builder = if self.inner.config.is_aio_enabled() {
            match rio::new() {
                Ok(ioring) => {
                    warn!("bob will start with AIO - async fs io api");
                    builder.enable_aio(ioring)
                }
                Err(e) => {
                    warn!("bob will start with standard sync fs io api");
                    warn!("can't start with AIO, cause: {}", e);
                    self.inner.config.set_aio(false);
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
            .with_context(|| format!("cannot build pearl by path: {:?}", &self.inner.disk_path))
    }

    pub async fn delete(&self, key: BobKey, _meta: &BobMeta, force_delete: bool) -> Result<u64, Error> {
        let state = self.storage.read().await;
        if let Some(storage) = state.get() {
            trace!("Vdisk: {}, delete key: {}", self.inner.vdisk, key);
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
            trace!("Vdisk: {} isn't ready for reading: {:?}", self.inner.vdisk, state);
            Err(Error::vdisk_is_not_ready())
        }
    }

    pub async fn close_storage(&self) {
        let mut pearl_sync = self.storage.write().await;
        if let Some(storage) = pearl_sync.reset() {
            if let Err(e) = storage.fsyncdata().await {
                warn!("pearl fsync error (path: '{}'): {:?}", self.inner.disk_path.display(), e);
            }
            if let Err(e) = storage.close().await {
                warn!("pearl close error (path: '{}'): {:?}", self.inner.disk_path.display(), e);
            }
        }
    }

    pub async fn disk_used(&self) -> u64 {
        let storage_guard = self.storage.read().await;
        if let Some(storage) = storage_guard.get() {
            storage.disk_used().await
        } else {
            0
        }
    }
}

#[async_trait::async_trait]
impl BloomProvider<Key> for Holder {
    type Filter = <Storage<Key> as BloomProvider<Key>>::Filter;
    async fn check_filter(&self, item: &Key) -> FilterResult {
        let storage = self.storage.read().await;
        if let Some(storage) = storage.get() {
            return BloomProvider::check_filter(storage, item).await;
        }
        FilterResult::NeedAdditionalCheck
    }

    fn check_filter_fast(&self, _item: &Key) -> FilterResult {
        FilterResult::NeedAdditionalCheck
    }

    async fn offload_buffer(&mut self, needed_memory: usize, level: usize) -> usize {
        let mut storage = self.storage.write().await;
        if let Some(storage) = storage.get_mut() {
            storage.offload_buffer(needed_memory, level).await
        } else {
            0
        }
    }

    async fn get_filter(&self) -> Option<Self::Filter> {
        let storage = self.storage.read().await;
        if let Some(storage) = storage.get() {
            storage.get_filter().await
        } else {
            None
        }
    }

    fn get_filter_fast(&self) -> Option<&Self::Filter> {
        None
    }

    async fn filter_memory_allocated(&self) -> usize {
        let storage = self.storage.read().await;
        if let Some(storage) = storage.get() {
            storage.filter_memory_allocated().await
        } else {
            0
        }
    }
}

#[derive(Debug)]
pub enum PearlState {
    // pearl starting
    Initializing,
    // pearl is started and working
    Running(PearlStorage),
}

#[derive(Debug)]
pub struct PearlSync {
    state: PearlState,
}
impl PearlSync {
    pub fn get(&self) -> Option<&PearlStorage> {
        match &self.state {
            PearlState::Initializing => None,
            PearlState::Running(storage) => Some(storage)
        }
    }

    pub fn get_mut(&mut self) -> Option<&mut PearlStorage> {
        match &mut self.state {
            PearlState::Initializing => None,
            PearlState::Running(storage) => Some(storage)
        }
    }

    #[inline]
    pub fn is_ready(&self) -> bool {
        matches!(&self.state, PearlState::Running(_))
    }

    pub fn set_ready(&mut self, storage: PearlStorage) -> Result<(), Error> {
        if self.is_ready() {
            return Err(Error::failed("Pearl storage already initialized. Please, close previous before"));
        }
        self.state = PearlState::Running(storage);
        Ok(())
    }

    pub fn reset(&mut self) -> Option<PearlStorage> {
        let prev_state = std::mem::replace(&mut self.state, PearlState::Initializing);
        match prev_state {
            PearlState::Initializing => None,
            PearlState::Running(storage) => Some(storage)
        }
    }
}

impl Default for PearlSync {
    fn default() -> Self {
        Self {
            state: PearlState::Initializing,
        }
    }
}
