use super::prelude::*;
use std::fs::remove_dir_all;

#[derive(Debug)]
pub(crate) struct SyncState {
    state: RwLock<StateWrapper>,
}
impl SyncState {
    pub(crate) fn new() -> Self {
        Self {
            state: RwLock::new(StateWrapper::new()),
        }
    }
    pub(crate) async fn mark_as_created(&self) {
        let mut st = self.state.write().await;
        st.created();
    }

    pub(crate) async fn try_init(&self) -> bool {
        let mut st = self.state.write().await;
        if st.is_creating() {
            trace!("New object is currently creating, state: {:?}", st);
            false
        } else {
            st.start();
            true
        }
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
    pub(crate) fn new() -> Self {
        Self {
            state: CreationState::No,
        }
    }

    #[inline]
    pub(crate) fn is_creating(&self) -> bool {
        self.state == CreationState::Creating
    }

    #[inline]
    pub(crate) fn start(&mut self) {
        self.state = CreationState::Creating;
    }

    #[inline]
    pub(crate) fn created(&mut self) {
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
