use crate::{
    pearl::{postprocessor::SimpleHolder, DiskController},
    prelude::*,
};

use super::core::BackendResult;

pub struct Stuff;

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

impl Stuff {
    pub async fn check_or_create_directory(path: &Path) -> BackendResult<()> {
        if path.exists() {
            trace!("directory: {:?} exists", path);
        } else {
            let dir = path
                .to_str()
                .ok_or_else(|| Error::storage("invalid some path, check vdisk or disk names"))?;

            create_dir_all(&path)
                .await
                .map(|_| info!("create directory: {}", dir))
                .map_err(|e| match e.kind() {
                    IOErrorKind::PermissionDenied | IOErrorKind::Other => {
                        Error::possible_disk_disconnection()
                    }
                    _ => Error::storage(format!(
                        "cannot create directory: {}, error: {}",
                        dir,
                        e.to_string()
                    )),
                })?;
            info!("dir created: {}", path.display());
        }
        Ok(())
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
                    NaiveDateTime::from_timestamp(time.try_into().unwrap(), 0),
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

    pub(crate) async fn offload_old_filters(
        iter: impl IntoIterator<Item = &Arc<DiskController>>,
        limit: usize,
    ) {
        let now = Instant::now();
        let mut holders = Self::collect_holders(iter).await;
        let mut current_size = holders.iter().map(|x| x.1).sum::<usize>();
        let initial_size = current_size;
        if current_size < limit {
            return;
        }
        holders.sort_by_key(|h| h.0.timestamp());
        let holders_count = holders.len();
        let mut freed = 0;
        for (holder, size) in holders {
            if current_size < limit {
                break;
            }
            holder.offload_filter().await;
            let new_size = holder.filter_memory_allocated().await;
            freed += size.saturating_sub(new_size);
            current_size = current_size.saturating_sub(size.saturating_sub(new_size));
        }
        let elapsed = now.elapsed();
        if freed != 0 {
            log::error!(
                "Filters offloaded in {}s for {} holders: {} -> {}, {} freed",
                elapsed.as_secs_f64(),
                holders_count,
                initial_size,
                current_size,
                freed
            );
        }
    }

    async fn collect_holders(
        iter: impl IntoIterator<Item = &Arc<DiskController>>,
    ) -> Vec<(SimpleHolder, usize)> {
        let mut res = vec![];
        for dc in iter {
            for group in dc.groups().read().await.iter() {
                for holder in group.holders().read().await.iter() {
                    let holder: SimpleHolder = holder.into();
                    if holder.is_ready().await {
                        let size = holder.filter_memory_allocated().await;
                        res.push((holder, size));
                    }
                }
            }
        }
        res
    }
}

pub fn get_current_timestamp() -> u64 {
    let now: DateTime<Utc> = DateTime::from(SystemTime::now());
    now.timestamp().try_into().unwrap()
}
