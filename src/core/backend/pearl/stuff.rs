use crate::core::backend::pearl::data::*;
use futures_locks::RwLock;

use futures::future::Future;
use futures03::{compat::Future01CompatExt, Future as Future03, FutureExt};

use std::{pin::Pin, sync::Arc, fs::create_dir_all, path::PathBuf};

pub(crate) struct LockGuard<TGuard> {
    storage: Arc<RwLock<TGuard>>,
}

impl<TGuard: Send + Clone> LockGuard<TGuard> {
    pub(crate) fn new(data: TGuard) -> Self {
        LockGuard {
            storage: Arc::new(RwLock::new(data)),
        }
    }

    pub(crate) async fn read<F, TRet, TErr>(&self, f: F) -> Result<TRet, TErr>
    where
        F: Fn(TGuard) -> Pin<Box<dyn Future03<Output = Result<TRet, TErr>> + Send>> + Send + Sync,
    {
        self.storage
            .read()
            .map(move |st| {
                let clone = st.clone();
                f(clone)
            })
            .map_err(|e| {
                error!("lock error: {:?}", e);
                e
            })
            .compat()
            .boxed()
            .await
            .unwrap()
            .await
    }

    pub(crate) async fn write_sync_mut<F, Ret>(&self, f: F) -> BackendResult<Ret>
    where
        F: Fn(&mut TGuard) -> Ret + Send + Sync,
    {
        self.storage
            .write()
            .map(move |mut st| {
                f(&mut *st)
            })
            .map_err(|e| {
                error!("lock error: {:?}", e);
                format!("lock error: {:?}", e)
            })
            .compat()
            .boxed()
            .await
    }
}

pub(crate) struct Stuff {}

impl Stuff {
    pub(crate) fn check_or_create_directory(path: &PathBuf) -> BackendResult<()> {
        if !path.exists() {
            return match path.to_str() {
                Some(dir) => create_dir_all(&path)
                    .map(|_r| {
                        info!("create directory: {}", dir);
                        ()
                    })
                    .map_err(|e| {
                        format!("cannot create directory: {}, error: {}", dir, e.to_string())
                    }),
                _ => Err("invalid some path, check vdisk or disk names".to_string()),
            };
        }
        trace!("directory: {:?} exists", path);
        Ok(())
    }
}