use super::prelude::*;

pub(crate) struct LockGuard<TGuard> {
    storage: Arc<RwLock<TGuard>>,
}

impl<TGuard: Send + Clone> LockGuard<TGuard> {
    pub(crate) fn new(data: TGuard) -> Self {
        LockGuard {
            storage: Arc::new(RwLock::new(data)),
        }
    }

    pub(crate) async fn read<F, TRet>(&self, f: F) -> BackendResult<TRet>
    where
        F: Fn(TGuard) -> Future03Result<TRet> + Send + Sync,
    {
        // let lock = self.storage.read().compat().boxed().await.map_err(|_e| {
        //     error!("cannot take lock");
        //     panic!("cannot take lock");
        // });

        // lock.map(move |st| {
        //     let clone = st.clone();
        //     f(clone)
        // })
        // .map_err(|e| {
        //     error!("lock error: {:?}", e);
        //     Error::StorageError(format!("lock error: {:?}", e))
        // })?
        // .await
        unimplemented!()
    }

    pub(crate) async fn write_sync_mut<F, Ret>(&self, f: F) -> BackendResult<Ret>
    where
        F: Fn(&mut TGuard) -> Ret + Send + Sync,
    {
        // let lock = self.storage.write().compat().boxed().await.map_err(|_e| {
        //     error!("cannot take lock");
        //     panic!("cannot take lock");
        // });

        // lock.map(move |mut st| f(&mut *st)).map_err(|e| {
        //     error!("lock error: {:?}", e);
        //     Error::StorageError(format!("lock error: {:?}", e))
        // })
        unimplemented!()
    }

    pub(crate) async fn write_mut<F, TRet>(&self, f: F) -> BackendResult<TRet>
    where
        F: Fn(&mut TGuard) -> Future03Result<TRet> + Send + Sync,
    {
        // let lock = self.storage.write().compat().boxed().await.map_err(|_e| {
        //     error!("cannot take lock");
        //     panic!("cannot take lock");
        // });

        // lock.map(move |mut st| f(&mut *st))
        //     .map_err(|e| {
        //         error!("lock error: {:?}", e);
        //         Error::StorageError(format!("lock error: {:?}", e))
        //     })?
        //     .await
        unimplemented!()
    }
}

pub(crate) struct SyncState {
    state: LockGuard<StateWrapper>,
}
impl SyncState {
    pub fn new() -> Self {
        SyncState {
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
        StateWrapper {
            state: CreationState::No,
        }
    }

    pub fn is_creating(&self) -> bool {
        self.state == CreationState::Creating
    }

    pub fn start(&mut self) {
        self.state = CreationState::Creating;
    }

    pub fn created(&mut self) {
        self.state = CreationState::No;
    }
}

impl std::fmt::Display for StateWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[{:?}]", self.state)
    }
}

pub(crate) struct Stuff {}

impl Stuff {
    pub(crate) fn check_or_create_directory(path: &PathBuf) -> BackendResult<()> {
        if !path.exists() {
            return match path.to_str() {
                Some(dir) => create_dir_all(&path)
                    .map(|_r| info!("create directory: {}", dir))
                    .map_err(|e| {
                        Error::StorageError(format!(
                            "cannot create directory: {}, error: {}",
                            dir,
                            e.to_string()
                        ))
                    }),
                _ => Err(Error::StorageError(
                    "invalid some path, check vdisk or disk names".to_string(),
                )),
            };
        }
        trace!("directory: {:?} exists", path);
        Ok(())
    }

    pub(crate) fn drop_pearl_lock_file(path: &PathBuf) -> BackendResult<()> {
        let mut file = path.clone();
        file.push("pearl.lock");
        if file.exists() {
            return remove_file(&file)
                .map(|_r| debug!("deleted lock file from directory: {:?}", file))
                .map_err(|e| {
                    Error::StorageError(format!(
                        "cannot delete lock file from directory: {:?}, error: {}",
                        file,
                        e.to_string()
                    ))
                });
        }
        Ok(())
    }

    pub(crate) fn get_start_timestamp_by_std_time(
        period: Duration,
        time: SystemTime,
    ) -> BackendResult<i64> {
        let period = ChronoDuration::from_std(period).map_err(|e| {
            trace!("smth wrong with time: {:?}, error: {}", period, e);
            Error::Failed(format!("smth wrong with time: {:?}, error: {}", period, e))
        })?;
        let time = DateTime::from(time);

        Self::get_start_timestamp(period, time)
    }

    pub(crate) fn get_start_timestamp_by_timestamp(
        period: Duration,
        time: i64,
    ) -> BackendResult<i64> {
        let period = ChronoDuration::from_std(period).map_err(|e| {
            trace!("smth wrong with time: {:?}, error: {}", period, e);
            Error::Failed(format!("smth wrong with time: {:?}, error: {}", period, e))
        })?;
        let time: DateTime<Utc> = DateTime::from_utc(NaiveDateTime::from_timestamp(time, 0), Utc);
        Self::get_start_timestamp(period, time)
    }

    fn get_start_timestamp(period: ChronoDuration, time: DateTime<Utc>) -> BackendResult<i64> {
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
        Ok(start_time.timestamp())
    }
    pub(crate) fn get_period_timestamp(period: Duration) -> BackendResult<i64> {
        let period = ChronoDuration::from_std(period).map_err(|e| {
            trace!("smth wrong with time: {:?}, error: {}", period, e);
            Error::Failed(format!("smth wrong with time: {:?}, error: {}", period, e))
        })?;

        Ok(period.num_seconds())
    }

    pub(crate) async fn wait(delay: Duration) {
        // delay_for(delay).compat().boxed().await;
        unimplemented!()
    }
}
