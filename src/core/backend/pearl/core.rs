use super::prelude::*;
use crate::core::backend::core::Exist;

#[derive(Clone, Debug)]
pub(crate) struct PearlBackend {
    settings: Arc<Settings>,

    vdisks_groups: Arc<Vec<PearlGroup>>,
    alien_vdisks_groups: Arc<LockGuard<Vec<PearlGroup>>>,
    // holds state when we create new alien pearl dir
    pearl_sync: Arc<SyncState>,
}

impl PearlBackend {
    pub(crate) fn new(mapper: Arc<Virtual>, config: &NodeConfig) -> Self {
        debug!("initializing pearl backend");
        let settings = Arc::new(Settings::new(config, mapper));

        let vdisks_groups = Arc::new(settings.clone().read_group_from_disk(config));
        trace!("count vdisk groups: {}", vdisks_groups.len());

        let alien = settings
            .clone()
            .read_alien_directory(config)
            .expect("vec of pearl groups");
        trace!("count alien vdisk groups: {}", alien.len());
        let alien_vdisks_groups = Arc::new(LockGuard::new(alien)); //TODO

        Self {
            settings,
            vdisks_groups,
            alien_vdisks_groups,
            pearl_sync: Arc::new(SyncState::new()),
        }
    }

    #[inline]
    async fn put_common(pearl: PearlGroup, key: BobKey, data: BobData) -> PutResult {
        let res = pearl.put(key, data).await;
        res.map(|_ok| BackendPutResult {})
    }

    async fn get_common(pearl: PearlGroup, key: BobKey) -> GetResult {
        trace!("GET[{}] try from: {}", key, pearl);
        let result = pearl.get(key).await;
        result.map(|get_res| BackendGetResult { data: get_res.data })
    }

    async fn create_alien_pearl(&self, operation: BackendOperation) -> BackendResult<()> {
        // check if pearl is currently creating
        trace!("try create alien for: {}", operation);
        if self.pearl_sync.try_init().await? {
            // check if alien created
            debug!("create alien for: {}", operation);
            if self.find_alien_pearl(operation.clone()).await.is_err() {
                let pearl = self
                    .settings
                    .clone()
                    .create_group(&operation)
                    .expect("pearl group"); //TODO

                self.alien_vdisks_groups
                    .write_sync_mut(|groups| {
                        groups.push(pearl.clone());
                    })
                    .await;
            }
            // if it run here then it will conflict withtimstamp runtime creation
            // pearl.run().await;
            self.pearl_sync.mark_as_created().await
        } else {
            let t = self.settings.config.settings().create_pearl_wait_delay();
            delay_for(t).await;
            Ok(())
        }
    }

    async fn find_alien_pearl(&self, operation: BackendOperation) -> BackendResult<PearlGroup> {
        self.alien_vdisks_groups
            .read(|pearls| {
                let op = operation.clone();
                async move {
                    pearls
                        .iter()
                        .find(|vd| vd.can_process_operation(&op))
                        .cloned()
                        .ok_or({
                            trace!("cannot find actual alien folder. {}", op);
                            Error::Failed(format!("cannot find actual alien folder. {}", op))
                        })
                }
                .boxed()
            })
            .await
    }
}

impl BackendStorage for PearlBackend {
    fn run_backend(&self) -> RunResult {
        debug!("run pearl backend");

        let vdisks_groups = self.vdisks_groups.clone();
        let alien_vdisks_groups = self.alien_vdisks_groups.clone();

        async move {
            for vdisk_group in vdisks_groups.iter() {
                vdisk_group.run().await;
            }

            alien_vdisks_groups
                .read(|pearl_groups| {
                    async move {
                        for group in pearl_groups {
                            group.run().await;
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

        let vdisk_group = self
            .vdisks_groups
            .iter()
            .find(|vd| vd.can_process_operation(&operation));

        if let Some(group) = vdisk_group {
            let group = group.clone();
            let task = async move {
                Self::put_common(group, key, data) // TODO remove copy of disk. add Box?
                    .await
                    .map_err(|e| {
                        debug!("PUT[{}], error: {:?}", key, e);
                        e
                    })
            }
            .boxed();
            Put(task)
        } else {
            debug!(
                "PUT[{}] to pearl backend. Cannot find group, operation: {}",
                key, operation
            );
            Put(future::err(Error::VDiskNoFound(operation.vdisk_id)).boxed())
        }
    }

    fn put_alien(&self, operation: BackendOperation, key: BobKey, data: BobData) -> Put {
        debug!("PUT[alien][{}] to pearl backend", key);

        let backend = self.clone();
        Put(async move {
            let mut vdisk_group = backend.find_alien_pearl(operation.clone()).await;
            if vdisk_group.is_err() {
                debug!("need create alien for: {}", operation.clone());
                backend
                    .create_alien_pearl(operation.clone())
                    .await
                    .expect("create alien pearl");
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
                Err(Error::VDiskNoFound(operation.vdisk_id))
            }
        }
        .boxed())
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
                future::err(Error::VDiskNoFound(operation.vdisk_id)).boxed()
            }
        })
    }

    fn get_alien(&self, operation: BackendOperation, key: BobKey) -> Get {
        debug!("Get[alien][{}] from pearl backend", key);

        let backend = self.clone();
        Get(async move {
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
                Err(Error::KeyNotFound(key))
            }
        }
        .boxed())
    }

    fn exist(&self, operation: BackendOperation, keys: &[BobKey]) -> Exist {
        let keys = keys.to_vec();
        let vdisk_group = self
            .vdisks_groups
            .iter()
            .find(|vd| vd.can_process_operation(&operation))
            .cloned();
        Exist(Box::pin(async move {
            if let Some(group) = vdisk_group {
                group.exist(&keys).await
            } else {
                Err(BackendError::Internal)
            }
        }))
    }

    fn exist_alien(&self, operation: BackendOperation, keys: &[BobKey]) -> Exist {
        let backend = self.clone();
        let keys = keys.to_vec();
        Exist(Box::pin(async move {
            let vdisk_group = backend.find_alien_pearl(operation.clone()).await;
            if let Ok(group) = vdisk_group {
                group.exist(&keys).await
            } else {
                Err(BackendError::Internal)
            }
        }))
    }

    fn vdisks_groups(&self) -> Option<&[PearlGroup]> {
        Some(&self.vdisks_groups)
    }
}
