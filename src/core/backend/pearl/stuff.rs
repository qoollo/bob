use crate::core::backend;
use crate::core::backend::pearl::data::*;

use futures03::{compat::Future01CompatExt, FutureExt};
use futures_locks::RwLock;

use std::{
    fs::{create_dir_all, remove_file},
    path::PathBuf,
    sync::Arc,
};

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
}
