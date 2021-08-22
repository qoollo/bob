use crate::{
    pearl::{holder::PearlSync, DiskController},
    prelude::*,
};

use super::core::BackendResult;

pub struct Stuff;

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

    pub fn get_start_timestamp_by_std_time(period: Duration, time: SystemTime) -> u64 {
        ChronoDuration::from_std(period)
            .map(|period| Self::get_start_timestamp(period, DateTime::from(time)))
            .map_err(|e| {
                trace!("smth wrong with time: {:?}, error: {}", period, e);
            })
            .expect("convert std time to chrono")
    }

    // @TODO remove cast as u64
    pub fn get_start_timestamp_by_timestamp(period: Duration, time: u64) -> u64 {
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

    pub(crate) async fn offload_old_filters(
        iter: impl IntoIterator<Item = &Arc<DiskController>>,
        limit: usize,
    ) {
        let now = Instant::now();
        let mut holders = Self::collect_holders(iter).await;
        let mut holders = holders
            .iter_mut()
            .map(|h| async { (h.filter_memory_allocated().await, h) })
            .collect::<FuturesUnordered<_>>()
            .fold(vec![], |mut acc, x| async move {
                acc.push(x);
                acc
            })
            .await;
        let mut current_size = holders.iter().map(|x| x.0).sum::<usize>();
        let initial_size = current_size;
        if current_size < limit {
            return;
        }
        holders.sort_by_key(|h| h.1.timestamp);
        let mut freed = 0;
        for (size, holder) in holders {
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
            log::info!(
                "Filters offloaded in {}s: {} -> {}, {} freed",
                elapsed.as_secs_f64(),
                initial_size,
                current_size,
                freed
            );
        }
    }

    async fn collect_holders(iter: impl IntoIterator<Item = &Arc<DiskController>>) -> Vec<Holder> {
        let mut res = vec![];
        for dc in iter {
            for group in dc.groups().read().await.iter() {
                for holder in group.holders().read().await.iter() {
                    let storage = holder.cloned_storage();
                    let timestamp = holder.end_timestamp();
                    if storage.read().await.is_ready() {
                        let holder = Holder { storage, timestamp };
                        res.push(holder);
                    }
                }
            }
        }
        res
    }
}

struct Holder {
    storage: Arc<RwLock<PearlSync>>,
    timestamp: u64,
}

impl Holder {
    async fn filter_memory_allocated(&self) -> usize {
        self.storage.read().await.filter_memory_allocated().await
    }

    async fn offload_filter(&self) {
        self.storage.read().await.offload_filters().await
    }
}

impl PartialOrd for Holder {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.timestamp.partial_cmp(&other.timestamp)
    }
}

impl PartialEq for Holder {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.storage, &other.storage) && self.timestamp == other.timestamp
    }
}

impl Eq for Holder {}

impl Ord for Holder {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}
