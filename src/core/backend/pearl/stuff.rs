use crate::core::backend;
use crate::core::backend::pearl::data::*;

use futures03::{compat::Future01CompatExt, FutureExt};
use futures_locks::RwLock;

use chrono::{DateTime, Datelike, Duration, Utc};
use std::{
    fs::{create_dir_all, remove_file},
    path::PathBuf,
    sync::Arc,
    time,
};
use tokio_timer::sleep;

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
        let lock = self.storage.read().compat().boxed().await.map_err(|_e| {
            error!("cannot take lock");
            panic!("cannot take lock");
        });

        lock.map(move |st| {
            let clone = st.clone();
            f(clone)
        })
        .map_err(|e| {
            error!("lock error: {:?}", e);
            backend::Error::StorageError(format!("lock error: {:?}", e))
        })?
        .await
    }

    pub(crate) async fn write_sync_mut<F, Ret>(&self, f: F) -> BackendResult<Ret>
    where
        F: Fn(&mut TGuard) -> Ret + Send + Sync,
    {
        let lock = self.storage.write().compat().boxed().await.map_err(|_e| {
            error!("cannot take lock");
            panic!("cannot take lock");
        });

        lock.map(move |mut st| f(&mut *st)).map_err(|e| {
            error!("lock error: {:?}", e);
            backend::Error::StorageError(format!("lock error: {:?}", e))
        })
    }

    pub(crate) async fn write_mut<F, TRet>(&self, f: F) -> BackendResult<TRet>
    where
        F: Fn(&mut TGuard) -> Future03Result<TRet> + Send + Sync,
    {
        let lock = self.storage.write().compat().boxed().await.map_err(|_e| {
            error!("cannot take lock");
            panic!("cannot take lock");
        });

        lock.map(move |mut st| f(&mut *st))
            .map_err(|e| {
                error!("lock error: {:?}", e);
                backend::Error::StorageError(format!("lock error: {:?}", e))
            })?
            .await
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
                        backend::Error::StorageError(format!(
                            "cannot create directory: {}, error: {}",
                            dir,
                            e.to_string()
                        ))
                    }),
                _ => Err(backend::Error::StorageError(
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
                    backend::Error::StorageError(format!(
                        "cannot delete lock file from directory: {:?}, error: {}",
                        file,
                        e.to_string()
                    ))
                });
        }
        Ok(())
    }

    pub(crate) fn get_start_timestamp(
        period: time::Duration,
        time: time::SystemTime,
    ) -> BackendResult<i64> {
        let period: Duration = Duration::from_std(period).map_err(|e| {
            trace!("smth wrong with time: {:?}, error: {}", period, e);
            backend::Error::Failed(format!("smth wrong with time: {:?}, error: {}", period, e))
        })?;
        let time: DateTime<Utc> = DateTime::from(time);

        let mut start_time = match period {
            period if period <= Duration::days(1) => time.date().and_hms(0, 0, 0),
            period if period <= Duration::weeks(1) => {
                let time = time.date().and_hms(0, 0, 0);
                time - Duration::days((time.weekday().num_days_from_monday() - 1) as i64)
            }
            _ => panic!("pearid: {} is too large", period),
        };

        while !(start_time <= time && time < start_time + period) {
            start_time = start_time + period;
        }
        Ok(start_time.timestamp())
    }

    pub(crate) fn get_period_timestamp(period: time::Duration) -> BackendResult<i64> {
        let period: Duration = Duration::from_std(period).map_err(|e| {
            trace!("smth wrong with time: {:?}, error: {}", period, e);
            backend::Error::Failed(format!("smth wrong with time: {:?}, error: {}", period, e))
        })?;

        Ok(period.num_seconds())
    }

    pub(crate) async fn wait(delay: time::Duration) {
        let _ = sleep(delay).compat().boxed().await;
    }
}
