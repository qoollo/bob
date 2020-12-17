use super::prelude::*;

pub(crate) struct Stuff;

impl Stuff {
    pub(crate) async fn check_or_create_directory(work_dir: &WorkDir) -> BackendResult<()> {
        if work_dir.as_path().exists() {
            trace!("directory: {:?} exists", work_dir);
            Ok(())
        } else if work_dir.device().map(|p| p.exists()) == Some(true) {
            let name = work_dir.as_path().to_string_lossy();
            Err(Error::mount_point_not_found(name))
        } else {
            Self::create_dir_all(work_dir.as_path()).await
        }
    }

    async fn create_dir_all(path: impl AsRef<Path>) -> BackendResult<()> {
        let path = path.as_ref();
        if let Err(e) = tokio::fs::create_dir_all(path).await {
            let msg = format!("cannot create directory: {:?}, error: {}", path, e);
            Err(Error::storage(msg))
        } else {
            info!("all dirs created: {:?}", path);
            Ok(())
        }
    }

    pub(crate) fn drop_pearl_lock_file(path: &Path) -> BackendResult<()> {
        let mut file = path.to_path_buf();
        file.push("pearl.lock");
        if file.exists() {
            remove_file(&file)
                .map(|_| debug!("deleted lock file from directory: {:?}", file))
                .map_err(|e| {
                    Error::storage(format!(
                        "cannot delete lock file from directory: {:?}, error: {}",
                        file, e
                    ))
                })
        } else {
            Ok(())
        }
    }

    pub(crate) fn drop_directory(path: &Path) -> BackendResult<()> {
        //@TODO switch to tokio
        std::fs::remove_dir_all(path)
            .map(|_| debug!("deleted directory {:?}", path))
            .map_err(|e| Error::storage(format!("error deleting directory {:?}, {}", path, e)))
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
                Error::failed(format!("smth wrong with time: {:?}, error: {}", period, e))
            })
            .map(|period| {
                let time = DateTime::from_utc(
                    NaiveDateTime::from_timestamp(time.try_into().unwrap(), 0),
                    Utc,
                );
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
        start_time.timestamp().try_into().unwrap()
    }
}
