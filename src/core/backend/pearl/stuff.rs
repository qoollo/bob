use crate::core::backend::pearl::data::*;
use futures_locks::RwLock;

use futures::future::Future;
use futures03::{
    compat::Future01CompatExt,
    Future as Future03, FutureExt,
};

use std::{
    pin::Pin,
    sync::Arc,
};

pub(crate) struct LockGuard<TGuard> {
    storage: Arc<RwLock<TGuard>>,
}

impl<TGuard: Send + Clone> LockGuard<TGuard> 
{
    pub(crate) fn new(data: TGuard) -> Self {
        LockGuard {
            storage: Arc::new(RwLock::new(data))
        }
    }

    pub(crate) async fn read<F, Ret>(&self, f:F) -> BackendResult<Ret>
        where F: Fn(TGuard) -> Pin<Box<dyn Future03<Output = BackendResult<Ret>>+Send>> + Send+Sync,
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

    // pub(crate) async fn update(&self, pearl: PearlStorage) -> BackendResult<()>
    //  {
    //     let mut storage = self.storage
    //         .write()
    //         .compat()
    //         .await
    //         .map_err(|e| {
    //             error!("lock error on pearl vdisk: {:?}", e);
    //             format!("lock error on pearl vdisk: {:?}", e)
    //         })?;

    //     storage.storage = pearl;
    //     Ok(())
    // }

    // pub(crate) async fn get<F, Ret>(&self, f:F) -> BackendResult<Ret>
    //     where F: Fn(&PearlVDisk) -> Ret + Send+Sync,
    //  {
    //     self.storage
    //         .read()
    //         .map(move |st| {
    //             f(&*st)
    //         })
    //         .map_err(|e| {
    //             error!("lock error on pearl vdisk: {:?}", e);
    //             format!("lock error on pearl vdisk: {:?}", e)
    //         })
    //         .compat()
    //         .boxed()
    //         .await
    // }
}