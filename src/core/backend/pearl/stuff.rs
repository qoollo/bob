use super::prelude::*;
use std::fs::remove_dir_all;

#[derive(Debug)]
pub(crate) struct LockGuard<T> {
    storage: Arc<RwLock<T>>,
}

impl<T: Send + Clone> LockGuard<T> {
    pub(crate) fn new(data: T) -> Self {
        Self {
            storage: Arc::new(RwLock::new(data)),
        }
    }

    pub(crate) fn storage(&self) -> &RwLock<T> {
        &self.storage
    }

    pub(crate) async fn read<F, TRet>(&self, f: F) -> BackendResult<TRet>
    where
        F: Fn(T) -> FutureResult<TRet> + Send + Sync,
    {
        let storage = self.storage.read().await;
        f(storage.clone()).await
    }

    pub(crate) async fn write_sync_mut<F, Ret>(&self, f: F) -> Ret
    where
        F: Fn(&mut T) -> Ret + Send + Sync,
    {
        let mut storage = self.storage.write().await;
        f(&mut storage)
    }

    pub(crate) async fn write_mut<F, TRet>(&self, f: F) -> BackendResult<TRet>
    where
        F: Fn(&mut T) -> FutureResult<TRet> + Send + Sync,
    {
        let mut st = self.storage.write().await;

        f(&mut st)
            .map_err(|e| {
                error!("lock error: {:?}", e);
                Error::Storage(format!("lock error: {:?}", e))
            })
            .await
    }
}

#[derive(Debug)]
pub(crate) struct SyncState {
    state: LockGuard<StateWrapper>,
}
impl SyncState {
    pub fn new() -> Self {
        Self {
            state: LockGuard::new(StateWrapper::new()),
        }
    }
    pub async fn mark_as_created(&self) -> BackendResult<()> {
        self.state
            .write_mut(|st| {
                st.created();
                future::ok(()).boxed()
            })
            .await
    }

    pub async fn try_init(&self) -> BackendResult<bool> {
        self.state
            .write_mut(|st| {
                if st.is_creating() {
                    trace!("New object is currently creating, state: {}", st);
                    return future::ok(false).boxed();
                }
                st.start();
                future::ok(true).boxed()
            })
            .await
    }
}

#[derive(Clone, PartialEq, Debug)]
enum CreationState {
    No,
    Creating,
}

#[derive(Clone, PartialEq, Debug)]
struct StateWrapper {
    state: CreationState,
}

impl StateWrapper {
    pub fn new() -> Self {
        Self {
            state: CreationState::No,
        }
    }

    #[inline]
    pub fn is_creating(&self) -> bool {
        self.state == CreationState::Creating
    }

    #[inline]
    pub fn start(&mut self) {
        self.state = CreationState::Creating;
    }

    #[inline]
    pub fn created(&mut self) {
        self.state = CreationState::No;
    }
}

impl Display for StateWrapper {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        f.debug_struct("StateWrapper")
            .field("state", &self.state)
            .field("..", &"some fields ommited")
            .finish()
    }
}

pub(crate) struct Stuff;

impl Stuff {
    pub(crate) fn check_or_create_directory(path: &Path) -> BackendResult<()> {
        if path.exists() {
            trace!("directory: {:?} exists", path);
            Ok(())
        } else {
            let dir = path.to_str().ok_or_else(|| {
                Error::Storage("invalid some path, check vdisk or disk names".to_string())
            })?;

            create_dir_all(&path)
                .map(|_| info!("create directory: {}", dir))
                .map_err(|e| {
                    Error::Storage(format!(
                        "cannot create directory: {}, error: {}",
                        dir,
                        e.to_string()
                    ))
                })
        }
    }

    pub(crate) fn drop_pearl_lock_file(path: &PathBuf) -> BackendResult<()> {
        let mut file = path.clone();
        file.push("pearl.lock");
        if file.exists() {
            remove_file(&file)
                .map(|_| debug!("deleted lock file from directory: {:?}", file))
                .map_err(|e| {
                    Error::Storage(format!(
                        "cannot delete lock file from directory: {:?}, error: {}",
                        file, e
                    ))
                })
        } else {
            Ok(())
        }
    }

    pub(crate) fn drop_directory(path: &PathBuf) -> BackendResult<()> {
        remove_dir_all(path)
            .map(|_| debug!("deleted directory {:?}", path))
            .map_err(|e| Error::Storage(format!("error deleting directory {:?}, {}", path, e)))
    }

    pub(crate) fn get_start_timestamp_by_std_time(period: Duration, time: SystemTime) -> u64 {
        ChronoDuration::from_std(period)
            .map(|period| Self::get_start_timestamp(period, DateTime::from(time)))
            .map_err(|e| {
                trace!("smth wrong with time: {:?}, error: {}", period, e);
            })
            .expect("convert std time to chrono")
    }

    // @TODO remove cast as u64
    pub(crate) fn get_start_timestamp_by_timestamp(period: Duration, time: u64) -> u64 {
        ChronoDuration::from_std(period)
            .map_err(|e| {
                trace!("smth wrong with time: {:?}, error: {}", period, e);
                Error::Failed(format!("smth wrong with time: {:?}, error: {}", period, e))
            })
            .map(|period| {
                let time = DateTime::from_utc(NaiveDateTime::from_timestamp(time as i64, 0), Utc);
                Self::get_start_timestamp(period, time)
            })
            .expect("convert std time to chrono") as u64
    }

    // @TODO remove cast as u64
    fn get_start_timestamp(period: ChronoDuration, time: DateTime<Utc>) -> u64 {
        let mut start_time = match period {
            period if period <= ChronoDuration::days(1) => time.date().and_hms(0, 0, 0),
            period if period <= ChronoDuration::weeks(1) => {
                let time = time.date().and_hms(0, 0, 0);
                time - ChronoDuration::days(i64::from(time.weekday().num_days_from_monday() - 1))
            }
            _ => panic!("pearid: {} is too large", period),
        };

        while !(start_time <= time && time < start_time + period) {
            start_time = start_time + period;
        }
        start_time.timestamp() as u64
    }

    pub(crate) fn get_period_timestamp(period: Duration) -> u64 {
        ChronoDuration::from_std(period)
            .map_err(|e| {
                trace!("smth wrong with time: {:?}, error: {}", period, e);
                Error::Failed(format!("smth wrong with time: {:?}, error: {}", period, e))
            })
            .map(|period| period.num_seconds())
            .expect("convert std time to chrono") as u64
    }
}
