use crate::core::backend;
use crate::core::backend::core::*;
use super::{
    data::*,
    settings::Settings,
    holder::{PearlHolder, PearlSync},
    group::PearlGroup,
};
use crate::core::configs::node::NodeConfig;
use crate::core::data::{BobData, BobKey, VDiskId};
use crate::core::mapper::VDiskMapper;

use futures03::{
    future::err as err03,
    task::Spawn,
    FutureExt,
};

use std::sync::Arc;

pub struct PearlBackend<TSpawner> {
    alien_dir: PearlHolder<TSpawner>,

    vdisks_groups: Arc<Vec<PearlGroup<TSpawner>>>,
}

impl<TSpawner: Spawn + Clone + Send + 'static + Unpin + Sync> PearlBackend<TSpawner> {
    pub fn new(mapper: Arc<VDiskMapper>, config: &NodeConfig, spawner: TSpawner) -> Self {
        debug!("initializing pearl backend");
        let pearl_config = config.pearl.clone().unwrap();


        let settings = Arc::new(Settings::new(config, mapper.clone()));

        //init alien storage
        let alien_dir = PearlHolder::new_alien(
            &pearl_config.alien_disk(),
            settings.alien_directory(),
            pearl_config.clone(),
            spawner.clone(),
        );

        let vdisks_groups = Arc::new(settings.read_group_from_disk(settings.clone(), pearl_config, spawner));
        PearlBackend {
            alien_dir,
            vdisks_groups,
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
        disk_name: String,
        vdisk_id: VDiskId,
        f: F,
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
        let result = pearl
            .get(key)
            .map(|r| r.map(|data| BackendGetResult { data: data.data }))
            .await;
        result
    }
}

impl<TSpawner: Spawn + Clone + Send + 'static + Unpin + Sync> BackendStorage
    for PearlBackend<TSpawner>
{
    fn run_backend(&self) -> RunResult {
        debug!("run pearl backend");

        // let vdisks = self.vdisks.clone();
        // let alien_dir = self.alien_dir.clone();
        let vdisks_groups = self.vdisks_groups.clone();

        let q = async move {
            // for i in 0..vdisks.len() {
            //     let _ = vdisks[i]
            //         .clone()
            //         .prepare_storage() //TODO add Box?
            //         .await;
            // }

            // let _ = alien_dir.prepare_storage().await;
            for i in 0..vdisks_groups.len() {
                vdisks_groups[i].run().await;
            }

            Ok(())
        };

        q.boxed()
    }

    fn put(&self, operation: BackendOperation, key: BobKey, data: BobData) -> Put {
        debug!("PUT[{}] to pearl backend. opeartion: {}", key, operation);
        let vdisk_groups = self.vdisks_groups.clone();

        Put({
            let vdisk_group = vdisk_groups
                .iter()
                .find(|vd| vd.equal(&operation.disk_name_local(), &operation.vdisk_id));
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

    fn put_alien(&self, _operation: BackendOperation, key: BobKey, data: BobData) -> Put {
        unimplemented!();
        // debug!("PUT[alien][{}] to pearl backend", key);

        // let alien_dir = self.alien_dir.clone();
        // Put({
        //     async move {
        //         Self::put_common(alien_dir.clone(), key, data) // TODO remove copy of disk. add Box?
        //             .await
        //             .map_err(|e| {
        //                 debug!("PUT[alien][{}], error: {:?}", key, e);
        //                 e
        //             })
        //     }
        //         .boxed()
        // })
    }

    fn get(&self, operation: BackendOperation, key: BobKey) -> Get {
        debug!("Get[{}] from pearl backend. operation: {}", key, operation);

        let vdisks_groups = self.vdisks_groups.clone();
        Get({
            let vdisk_group = vdisks_groups
                .iter()
                .find(|vd| vd.equal(&operation.disk_name_local(), &operation.vdisk_id));
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

    fn get_alien(&self, _operation: BackendOperation, key: BobKey) -> Get {

        unimplemented!();
        // debug!("Get[alien][{}] from pearl backend", key);

        // let alien_dir = self.alien_dir.clone();
        // Get({
        //     async move {
        //         Self::get_common(alien_dir.clone(), key) // TODO remove copy of disk. add Box?
        //             .await
        //             .map_err(|e| {
        //                 debug!("PUT[alien][{}], error: {:?}", key, e);
        //                 e
        //             })
        //     }
        //         .boxed()
        // })
    }
}