use crate::core::backend;
use crate::core::backend::core::*;
use super::{
    data::*,
    metrics::*,
    stuff::{LockGuard, Stuff},
    settings::Settings,
    holder::{PearlHolder, PearlSync},
};
use crate::core::configs::node::{NodeConfig, PearlConfig};
use crate::core::data::{BobData, BobKey, VDiskId};
use crate::core::mapper::VDiskMapper;

use futures03::{
    future::err as err03,
    task::Spawn,
    FutureExt,
};

use std::sync::Arc;

pub struct PearlBackend<TSpawner> {
    vdisks: Arc<Vec<PearlHolder<TSpawner>>>,
    alien_dir: PearlHolder<TSpawner>,
}

impl<TSpawner: Spawn + Clone + Send + 'static + Unpin + Sync> PearlBackend<TSpawner> {
    pub fn new(mapper: Arc<VDiskMapper>, config: &NodeConfig, spawner: TSpawner) -> Self {
        debug!("initializing pearl backend");
        let pearl_config = config.pearl.clone().unwrap();

        let mut result = Vec::new();

        let settings = Settings::new(config, mapper.clone());

        //init pearl storages for each vdisk
        for disk in mapper.local_disks().iter() {
            let mut vdisks: Vec<PearlHolder<TSpawner>> = mapper
                .get_vdisks_by_disk(&disk.name)
                .iter()
                .map(|vdisk_id| {
                    PearlHolder::new(
                        &disk.name,
                        vdisk_id.clone(),
                        settings.normal_directory(&disk.path, vdisk_id),
                        pearl_config.clone(),
                        spawner.clone(),
                    )
                })
                .collect();
            result.append(&mut vdisks);
        }

        //init alien storage
        let alien_dir = PearlHolder::new_alien(
            &pearl_config.alien_disk(),
            settings.alien_directory(),
            pearl_config.clone(),
            spawner.clone(),
        );

        PearlBackend {
            vdisks: Arc::new(result),
            alien_dir,
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn test<TRet, F>(
        &self,
        disk_name: String,
        vdisk_id: VDiskId,
        f: F,
    ) -> BackendResult<TRet>
    where
        F: Fn(&mut PearlSync) -> TRet + Send + Sync,
    {
        let vdisks = self.vdisks.clone();
        let vdisk = vdisks.iter().find(|vd| vd.equal(&disk_name, &vdisk_id));
        if let Some(disk) = vdisk {
            let d_clone = disk.clone(); // TODO remove copy of disk. add Box?
            let q = async move { d_clone.test(f).await };
            q.await
        } else {
            Err(backend::Error::StorageError(format!(
                "vdisk not found: {}",
                vdisk_id
            )))
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn test_vdisk<TRet, F>(
        &self,
        disk_name: String,
        vdisk_id: VDiskId,
        f: F,
    ) -> BackendResult<TRet>
    where
        F: Fn(PearlHolder<TSpawner>) -> Future03Result<TRet> + Send + Sync,
    {
        let vdisks = self.vdisks.clone();
        let vdisk = vdisks.iter().find(|vd| vd.equal(&disk_name, &vdisk_id));
        if let Some(disk) = vdisk {
            let d_clone = disk.clone(); // TODO remove copy of disk. add Box?
            f(d_clone).await
        } else {
            async move {
                Err(backend::Error::StorageError(format!(
                    "vdisk not found: {}",
                    vdisk_id
                )))
            }
                .await
        }
    }

    fn is_write_error(err: Option<&backend::Error>) -> bool {
        match err {
            Some(backend::Error::DuplicateKey) | Some(backend::Error::VDiskIsNotReady) => false,
            Some(_) => true,
            _ => false,
        }
    }

    fn is_read_error(err: Option<&backend::Error>) -> bool {
        match err {
            Some(backend::Error::KeyNotFound) | Some(backend::Error::VDiskIsNotReady) => false,
            Some(_) => true,
            _ => false,
        }
    }

    async fn put_common(pearl: PearlHolder<TSpawner>, key: BobKey, data: BobData) -> PutResult {
        let result = pearl
            .write(key, Box::new(data))
            .map(|r| r.map(|_ok| BackendPutResult {}))
            .await;
        if Self::is_write_error(result.as_ref().err()) && pearl.try_reinit().await.unwrap() {
            let _ = pearl.reinit_storage().await;
        }
        result
    }

    async fn get_common(pearl: PearlHolder<TSpawner>, key: BobKey) -> GetResult {
        let result = pearl
            .read(key)
            .map(|r| r.map(|data| BackendGetResult { data }))
            .await;
        if Self::is_read_error(result.as_ref().err()) && pearl.try_reinit().await.unwrap() {
            let _ = pearl.reinit_storage().await;
        }
        result
    }
}

impl<TSpawner: Spawn + Clone + Send + 'static + Unpin + Sync> BackendStorage
    for PearlBackend<TSpawner>
{
    fn run_backend(&self) -> RunResult {
        debug!("run pearl backend");

        let vdisks = self.vdisks.clone();
        let alien_dir = self.alien_dir.clone();
        let q = async move {
            for i in 0..vdisks.len() {
                let _ = vdisks[i]
                    .clone()
                    .prepare_storage() //TODO add Box?
                    .await;
            }

            let _ = alien_dir.prepare_storage().await;
            Ok(())
        };

        q.boxed()
    }

    fn put(&self, operation: BackendOperation, key: BobKey, data: BobData) -> Put {
        debug!("PUT[{}] to pearl backend. opeartion: {}", key, operation);

        let vdisks = self.vdisks.clone();
        Put({
            let vdisk = vdisks
                .iter()
                .find(|vd| vd.equal(&operation.disk_name_local(), &operation.vdisk_id));
            if let Some(disk) = vdisk {
                let d_clone = disk.clone();
                async move {
                    Self::put_common(d_clone, key, data) // TODO remove copy of disk. add Box?
                        .await
                        .map_err(|e| {
                            debug!("PUT[{}], error: {:?}", key, e);
                            e
                        })
                }
                    .boxed()
            } else {
                debug!(
                    "PUT[{}] to pearl backend. Cannot find storage, operation: {}",
                    key, operation
                );
                err03(backend::Error::VDiskNoFound(operation.vdisk_id)).boxed()
            }
        })
    }

    fn put_alien(&self, _operation: BackendOperation, key: BobKey, data: BobData) -> Put {
        debug!("PUT[alien][{}] to pearl backend", key);

        let alien_dir = self.alien_dir.clone();
        Put({
            async move {
                Self::put_common(alien_dir.clone(), key, data) // TODO remove copy of disk. add Box?
                    .await
                    .map_err(|e| {
                        debug!("PUT[alien][{}], error: {:?}", key, e);
                        e
                    })
            }
                .boxed()
        })
    }

    fn get(&self, operation: BackendOperation, key: BobKey) -> Get {
        debug!("Get[{}] from pearl backend. operation: {}", key, operation);

        let vdisks = self.vdisks.clone();
        Get({
            let vdisk = vdisks
                .iter()
                .find(|vd| vd.equal(&operation.disk_name_local(), &operation.vdisk_id));
            if let Some(disk) = vdisk {
                let d_clone = disk.clone();
                async move {
                    Self::get_common(d_clone, key) // TODO remove copy of disk. add Box?
                        .await
                        .map_err(|e| {
                            debug!("GET[{}], error: {:?}", key, e);
                            e
                        })
                }
                    .boxed()
            } else {
                debug!(
                    "GET[{}] to pearl backend. Cannot find storage, operation: {}",
                    key, operation
                );
                err03(backend::Error::VDiskNoFound(operation.vdisk_id)).boxed()
            }
        })
    }

    fn get_alien(&self, _operation: BackendOperation, key: BobKey) -> Get {
        debug!("Get[alien][{}] from pearl backend", key);

        let alien_dir = self.alien_dir.clone();
        Get({
            async move {
                Self::get_common(alien_dir.clone(), key) // TODO remove copy of disk. add Box?
                    .await
                    .map_err(|e| {
                        debug!("PUT[alien][{}], error: {:?}", key, e);
                        e
                    })
            }
                .boxed()
        })
    }
}