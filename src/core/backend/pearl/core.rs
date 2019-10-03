use super::{
    data::*,
    group::PearlGroup,
    holder::{PearlHolder, PearlSync},
    settings::Settings,
    stuff::{LockGuard, Stuff, SyncState},
};
use crate::core::backend;
use crate::core::backend::core::*;
use crate::core::configs::node::NodeConfig;
use crate::core::data::{BobData, BobKey, VDiskId};
use crate::core::mapper::VDiskMapper;

use futures03::{future::err as err03, task::Spawn, FutureExt};

use std::sync::Arc;

#[derive(Clone)]
pub struct PearlBackend<TSpawner> {
    settings: Arc<Settings<TSpawner>>,

    vdisks_groups: Arc<Vec<PearlGroup<TSpawner>>>,
    alien_vdisks_groups: Arc<LockGuard<Vec<PearlGroup<TSpawner>>>>,
    // holds state when we create new alien pearl dir
    pearl_sync: Arc<SyncState>,
}

impl<TSpawner: Spawn + Clone + Send + 'static + Unpin + Sync> PearlBackend<TSpawner> {
    pub fn new(mapper: Arc<VDiskMapper>, config: &NodeConfig, spawner: TSpawner) -> Self {
        debug!("initializing pearl backend");
        let settings = Arc::new(Settings::new(config, mapper.clone(), spawner.clone()));

        let vdisks_groups =
            Arc::new(settings.read_group_from_disk(settings.clone(), config, spawner.clone()));
        trace!("count vdisk groups: {}", vdisks_groups.len());

        let alien = settings
            .read_alien_directory(settings.clone(), config, spawner.clone())
            .unwrap();
        trace!("count alien vdisk groups: {}", alien.len());
        let alien_vdisks_groups = Arc::new(LockGuard::new(alien)); //TODO

        PearlBackend {
            settings,
            vdisks_groups,
            alien_vdisks_groups,
            pearl_sync: Arc::new(SyncState::new()),
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn test<TRet, F>(
        &self,
        _disk_name: String,
        _vdisk_id: VDiskId,
        _f: F,
    ) -> BackendResult<TRet>
    where
        F: Fn(&mut PearlSync) -> TRet + Send + Sync,
    {
        unimplemented!();
        // let vdisks = self.vdisks.clone();
        // let vdisk = vdisks.iter().find(|vd| vd.equal(&disk_name, &vdisk_id));
        // if let Some(disk) = vdisk {
        //     let d_clone = disk.clone(); // TODO remove copy of disk. add Box?
        //     let q = async move { d_clone.test(f).await };
        //     q.await
        // } else {
        //     Err(backend::Error::StorageError(format!(
        //         "vdisk not found: {}",
        //         vdisk_id
        //     )))
        // }
    }

    #[allow(dead_code)]
    pub(crate) async fn test_vdisk<TRet, F>(
        &self,
        _disk_name: String,
        _vdisk_id: VDiskId,
        _f: F,
    ) -> BackendResult<TRet>
    where
        F: Fn(PearlHolder<TSpawner>) -> Future03Result<TRet> + Send + Sync,
    {
        unimplemented!();
        // let vdisks = self.vdisks.clone();
        // let vdisk = vdisks.iter().find(|vd| vd.equal(&disk_name, &vdisk_id));
        // if let Some(disk) = vdisk {
        //     let d_clone = disk.clone(); // TODO remove copy of disk. add Box?
        //     f(d_clone).await
        // } else {
        //     async move {
        //         Err(backend::Error::StorageError(format!(
        //             "vdisk not found: {}",
        //             vdisk_id
        //         )))
        //     }
        //         .await
        // }
    }

    async fn put_common(pearl: PearlGroup<TSpawner>, key: BobKey, data: BobData) -> PutResult {
        let result = pearl
            .put(key, data)
            .map(|r| r.map(|_ok| BackendPutResult {}))
            .await;
        result
    }

    async fn get_common(pearl: PearlGroup<TSpawner>, key: BobKey) -> GetResult {
        trace!("GET[{}] try from: {}", key, pearl);
        let result = pearl
            .get(key)
            .map(|r| r.map(|data| BackendGetResult { data: data.data }))
            .await;
        result
    }

    async fn create_alien_pearl(&self, operation: BackendOperation) -> BackendResult<()> {
        // check if pearl is currently creating
        trace!("try create alien for: {}", operation.clone());
        if self.pearl_sync.try_init().await? {
            // check if alien created
            debug!("create alien for: {}", operation.clone());
            if self.find_alien_pearl(operation.clone()).await.is_ok() {
                debug!("find new alien for: {}", operation.clone());
                let _ = self.pearl_sync.mark_as_created().await;
                return Ok(());
            }
            let pearl = self
                .settings
                .create_group(operation.clone(), self.settings.clone())
                .unwrap(); //TODO

            let _ = self
                .alien_vdisks_groups
                .write_sync_mut(|groups| {
                    groups.push(pearl.clone());
                })
                .await;

            // if it run here then it will conflict withtimstamp runtime creation
            // let _ = pearl.run().await;
            let _ = self.pearl_sync.mark_as_created().await?;
        } else {
            let delay = self.settings.config.settings().create_pearl_wait_delay();
            let _ = Stuff::wait(delay).await;
        }
        Ok(())
    }

    async fn find_alien_pearl(
        &self,
        operation: BackendOperation,
    ) -> BackendResult<PearlGroup<TSpawner>> {
        self.alien_vdisks_groups
            .read(|pearls| {
                let op = operation.clone();
                async move {
                    pearls
                        .iter()
                        .find(|vd| vd.can_process_operation(&op))
                        .map(|r| r.clone())
                        .ok_or({
                            trace!("cannot find actual alien folder. {}", op);
                            backend::Error::Failed(format!(
                                "cannot find actual alien folder. {}",
                                op
                            ))
                        })
                }
                    .boxed()
            })
            .await
    }
}

impl<TSpawner: Spawn + Clone + Send + 'static + Unpin + Sync> BackendStorage
    for PearlBackend<TSpawner>
{
    fn run_backend(&self) -> RunResult {
        debug!("run pearl backend");

        let vdisks_groups = self.vdisks_groups.clone();
        let alien_vdisks_groups = self.alien_vdisks_groups.clone();

        async move {
            for i in 0..vdisks_groups.len() {
                vdisks_groups[i].run().await;
            }

            alien_vdisks_groups
                .read(|pearls| {
                    async move {
                        for i in 0..pearls.len() {
                            pearls[i].run().await;
                        }
                        Ok(())
                    }
                        .boxed()
                })
                .await
        }
            .boxed()
    }

    fn put(&self, operation: BackendOperation, key: BobKey, data: BobData) -> Put {
        debug!("PUT[{}] to pearl backend. opeartion: {}", key, operation);
        let vdisk_groups = self.vdisks_groups.clone();

        Put({
            let vdisk_group = vdisk_groups
                .iter()
                .find(|vd| vd.can_process_operation(&operation));
            if let Some(group) = vdisk_group {
                let d_clone = group.clone();
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
                    "PUT[{}] to pearl backend. Cannot find group, operation: {}",
                    key, operation
                );
                err03(backend::Error::VDiskNoFound(operation.vdisk_id)).boxed()
            }
        })
    }

    fn put_alien(&self, operation: BackendOperation, key: BobKey, data: BobData) -> Put {
        debug!("PUT[alien][{}] to pearl backend", key);

        let backend = self.clone();
        Put({
            async move {
                let mut vdisk_group = backend.find_alien_pearl(operation.clone()).await;
                if vdisk_group.is_err() {
                    debug!("need create alien for: {}", operation.clone());
                    let _ = backend.create_alien_pearl(operation.clone()).await;
                    vdisk_group = backend.find_alien_pearl(operation.clone()).await;
                }
                if let Ok(group) = vdisk_group {
                    Self::put_common(group.clone(), key, data) // TODO remove copy of disk. add Box?
                        .await
                        .map_err(|e| {
                            debug!("PUT[alien][{}], error: {:?}", key, e);
                            e
                        })
                } else {
                    debug!(
                        "PUT[{}] to pearl backend. Cannot find group, operation: {}",
                        key, operation
                    );
                    Err(backend::Error::VDiskNoFound(operation.vdisk_id))
                }
            }
                .boxed()
        })
    }

    fn get(&self, operation: BackendOperation, key: BobKey) -> Get {
        debug!("Get[{}] from pearl backend. operation: {}", key, operation);

        let vdisks_groups = self.vdisks_groups.clone();
        Get({
            let vdisk_group = vdisks_groups
                .iter()
                .find(|vd| vd.can_process_operation(&operation));
            if let Some(group) = vdisk_group {
                let d_clone = group.clone();
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

    fn get_alien(&self, operation: BackendOperation, key: BobKey) -> Get {
        debug!("Get[alien][{}] from pearl backend", key);

        let backend = self.clone();
        Get({
            async move {
                let vdisk_group = backend.find_alien_pearl(operation.clone()).await;
                if let Ok(group) = vdisk_group {
                    Self::get_common(group.clone(), key) // TODO remove copy of disk. add Box?
                        .await
                        .map_err(|e| {
                            debug!("GET[alien][{}], error: {:?}", key, e);
                            e
                        })
                } else {
                    debug!(
                        "GET[alien][{}] to pearl backend. Cannot find storage, operation: {}",
                        key, operation
                    );
                    // must return that data not found
                    Err(backend::Error::KeyNotFound)
                }
            }
                .boxed()
        })
    }
}
