use crate::{pearl::hooks::SimpleHolder, prelude::*};

use super::core::BackendResult;

pub struct Utils;

pub struct StartTimestampConfig {
    round: bool,
}

impl Default for StartTimestampConfig {
    fn default() -> Self {
        Self { round: true }
    }
}

impl StartTimestampConfig {
    pub fn new(round: bool) -> Self {
        Self { round }
    }
}

impl Utils {
    /// Creates directory. Parent directory should exist
    pub async fn check_or_create_directory(path: &Path) -> BackendResult<()> {
        if path.exists() {
            trace!("directory: {} exists", path.display());
            return Ok(());
        } 

        let dir = path
            .to_str()
            .ok_or_else(|| Error::storage("invalid some path, check vdisk or disk names"))?;

        match tokio::fs::create_dir(&path).await {
            Ok(_) => {
                info!("dir created: {}", path.display());
                Ok(())
            },
            Err(err) => {
                match err.kind() {
                    IOErrorKind::AlreadyExists => {
                        trace!("directory: {} exists", path.display());
                        Ok(())
                    },
                    IOErrorKind::NotFound => {
                        Err(Error::storage(format!("cannot create directory, because part of the path is not exist: '{}', error: {}", dir, err)))
                    },
                    IOErrorKind::PermissionDenied | IOErrorKind::Other => {
                        Err(Error::possible_disk_disconnection())
                    },
                    _ => {
                        Err(Error::storage(format!("cannot create directory: '{}', error: {}", dir, err)))
                    }
                }
            }
        }
    }

    /// Creates directory and all its parents. If `base_path` is specified, then it should exist before creation started
    pub async fn check_or_create_all_directories(path: &Path, base_path: Option<&Path>) -> BackendResult<()> {
        if path.exists() {
            trace!("directory: {:?} exists", path);
            return Ok(());
        } 

        if let Some(base_path) = base_path {
            if !base_path.exists() {
                return Err(Error::storage(format!("cannot create directory, because base path is not exist. Dir: '{}', Base path: '{}'", path.display(), base_path.display())));
            }
        }

        let dir = path
            .to_str()
            .ok_or_else(|| Error::storage("invalid some path, check vdisk or disk names"))?;

        match tokio::fs::create_dir_all(&path).await {
            Ok(_) => {
                info!("dir created: {}", path.display());
                Ok(())
            },
            Err(err) => {
                match err.kind() {
                    IOErrorKind::PermissionDenied | IOErrorKind::Other => {
                        Err(Error::possible_disk_disconnection())
                    },
                    _ => {
                        Err(Error::storage(format!("cannot create directory: {}, error: {}", dir, err)))
                    }
                }
            }
        }
    }

    pub async fn drop_pearl_lock_file(path: &Path) -> BackendResult<()> {
        let mut file = path.to_owned();
        file.push("pearl.lock");
        if file.exists() {
            remove_file(&file).await.map_err(|e| {
                Error::storage(format!(
                    "cannot delete lock file from directory: {:?}, error: {}",
                    file, e
                ))
            })?;
            debug!("deleted lock file from directory: {:?}", file);
        }
        Ok(())
    }

    pub async fn drop_directory(path: &Path) -> BackendResult<()> {
        if let Err(e) = remove_dir_all(path).await {
            let e = Error::storage(format!("error deleting directory {:?}, {}", path, e));
            Err(e)
        } else {
            debug!("deleted directory {:?}", path);
            Ok(())
        }
    }

    pub fn get_start_timestamp_by_std_time(
        period: Duration,
        time: SystemTime,
        config: &StartTimestampConfig,
    ) -> u64 {
        ChronoDuration::from_std(period)
            .map(|period| Self::get_start_timestamp(period, DateTime::from(time), config))
            .map_err(|e| {
                trace!("smth wrong with time: {:?}, error: {}", period, e);
            })
            .expect("convert std time to chrono")
    }

    // @TODO remove cast as u64
    pub fn get_start_timestamp_by_timestamp(
        period: Duration,
        time: u64,
        config: &StartTimestampConfig,
    ) -> u64 {
        ChronoDuration::from_std(period)
            .map_err(|e| {
                trace!("smth wrong with time: {:?}, error: {}", period, e);
                Error::failed(format!("smth wrong with time: {:?}, error: {}", period, e))
            })
            .map(|period| {
                let time = DateTime::from_utc(
                    NaiveDateTime::from_timestamp_opt(time.try_into().unwrap(), 0).expect("time out of range"),
                    Utc,
                );
                Self::get_start_timestamp(period, time, config)
            })
            .expect("convert std time to chrono") as u64
    }

    // @TODO remove cast as u64
    fn get_start_timestamp(
        period: ChronoDuration,
        time: DateTime<Utc>,
        config: &StartTimestampConfig,
    ) -> u64 {
        if !config.round {
            return time.timestamp().try_into().unwrap();
        }
        let time = time.naive_local();
        let mut start_time = match period {
            period if period <= ChronoDuration::days(1) => time.date().and_hms_opt(0, 0, 0).unwrap(),
            period if period <= ChronoDuration::weeks(1) => {
                let time = time.date().and_hms_opt(0, 0, 0).unwrap();
                time - ChronoDuration::days(i64::from(time.weekday().num_days_from_monday() - 1))
            }
            _ => panic!("pearid: {} is too large", period),
        };

        while !(start_time <= time && time < start_time + period) {
            start_time = start_time + period;
        }
        start_time.timestamp().try_into().unwrap()
    }

    pub(crate) async fn offload_old_filters(mut holders: Vec<SimpleHolder>, limit: usize) {
        let now = Instant::now();
        let mut current_size = holders
            .iter()
            .map(|x| x.filter_memory_allocated())
            .collect::<FuturesUnordered<_>>()
            .fold(0, |acc, curr| async move { acc + curr })
            .await;
        let initial_size = current_size;
        if current_size < limit {
            info!(
                "Skip filter offloading, currently allocated: {}",
                current_size
            );
            return;
        }
        holders.sort_by_key(|h| h.timestamp());
        let mut freed_total = 0;
        for level in [0, 1] {
            for holder in holders.iter() {
                if current_size < limit {
                    break;
                }
                let freed = holder
                    .offload_filter(current_size.saturating_sub(limit), level)
                    .await;
                freed_total += freed;
                current_size = current_size.saturating_sub(freed);
            }
        }
        let elapsed = now.elapsed();
        info!(
            "Filters offloaded in {}s for {} holders: {} -> {}, {} freed",
            elapsed.as_secs_f64(),
            holders.len(),
            initial_size,
            current_size,
            freed_total
        );
    }
}

pub fn get_current_timestamp() -> u64 {
    let now: DateTime<Utc> = DateTime::from(SystemTime::now());
    now.timestamp().try_into().unwrap()
}
