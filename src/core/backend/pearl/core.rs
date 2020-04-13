use super::prelude::*;

pub(crate) type BackendResult<T> = std::result::Result<T, Error>;
pub(crate) type PearlStorage = Storage<Key>;

#[derive(Clone, Debug)]
pub(crate) struct Pearl {
    settings: Arc<Settings>,
    vdisks_groups: Arc<Vec<Group>>,
    alien_vdisks_groups: Arc<RwLock<Vec<Group>>>,
    pearl_sync: Arc<SyncState>, // holds state when we create new alien pearl dir
}

impl Pearl {
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
        let alien_vdisks_groups = Arc::new(RwLock::new(alien)); //TODO

        Self {
            settings,
            vdisks_groups,
            alien_vdisks_groups,
            pearl_sync: Arc::new(SyncState::new()),
        }
    }

    fn put_common(pearl: Group, key: BobKey, data: BobData) -> Put {
        debug!("PUT[{}] try to: {}", key, pearl);
        async move { pearl.put(key, data).await }.boxed()
    }

    fn get_common(pearl: Group, key: BobKey) -> Get {
        debug!("GET[{}] try from: {}", key, pearl);
        async move { pearl.get(key).await }.boxed()
    }

    async fn create_alien_pearl(&self, operation: BackendOperation) -> BackendResult<()> {
        // check if pearl is currently creating
        if self.pearl_sync.try_init().await {
            // check if alien created
            debug!("create alien for: {:?}", operation);
            if self.find_alien_pearl(operation.clone()).await.is_err() {
                let pearl = self
                    .settings
                    .clone()
                    .create_group(&operation)
                    .expect("pearl group");
                let mut groups = self.alien_vdisks_groups.write().await;
                groups.push(pearl.clone());
            }
            self.pearl_sync.mark_as_created().await;
        }
        Ok(())
    }

    async fn find_alien_pearl(&self, operation: BackendOperation) -> BackendResult<Group> {
        let operation = operation.clone();
        let pearls = self.alien_vdisks_groups.read().await;
        pearls
            .iter()
            .find(|group| group.can_process_operation(&operation))
            .cloned()
            .ok_or_else(|| {
                Error::Failed(format!("cannot find actual alien folder. {:?}", operation))
            })
    }
}

impl BackendStorage for Pearl {
    fn run_backend(&self) -> Run {
        debug!("run pearl backend");
        let vdisks_groups = self.vdisks_groups.clone();
        let alien_vdisks_groups = self.alien_vdisks_groups.clone();

        async move {
            for vdisk_group in vdisks_groups.iter() {
                vdisk_group.run().await;
            }
            let pearl_groups = alien_vdisks_groups.read().await;
            for group in pearl_groups.iter() {
                group.run().await;
            }
            Ok(())
        }
        .boxed()
    }

    fn put(&self, operation: BackendOperation, key: BobKey, data: BobData) -> Put {
        debug!("PUT[{}] to pearl backend. opeartion: {:?}", key, operation);
        let vdisk_group = self
            .vdisks_groups
            .iter()
            .find(|vd| vd.can_process_operation(&operation));

        if let Some(group) = vdisk_group {
            let group = group.clone();
            let task = async move {
                let res = Self::put_common(group, key, data).await;
                if let Err(e) = &res {
                    debug!("PUT[{}], error: {:?}", key, e);
                }
                res
            };
            task.boxed()
        } else {
            debug!(
                "PUT[{}] to pearl backend. Cannot find group, operation: {:?}",
                key, operation
            );
            future::err(Error::VDiskNotFound(operation.vdisk_id())).boxed()
        }
    }

    fn put_alien(&self, operation: BackendOperation, key: BobKey, data: BobData) -> Put {
        debug!("PUT[alien][{}] to pearl backend", key);
        let backend = self.clone();
        let task = async move {
            let mut vdisk_group = backend.find_alien_pearl(operation.clone()).await;
            if vdisk_group.is_err() {
                debug!("need create alien for: {:?}", operation);
                backend
                    .create_alien_pearl(operation.clone())
                    .await
                    .expect("create alien pearl");
                vdisk_group = backend.find_alien_pearl(operation.clone()).await;
            }
            if let Ok(group) = vdisk_group {
                let res = Self::put_common(group.clone(), key, data).await;
                if let Err(e) = &res {
                    debug!("PUT[alien][{}], error: {:?}", key, e);
                }
                res
            } else {
                debug!(
                    "PUT[alien][{}] to pearl backend. Cannot find group, operation: {:?}",
                    key, operation
                );
                Err(Error::VDiskNotFound(operation.vdisk_id()))
            }
        };
        task.boxed()
    }

    fn get(&self, operation: BackendOperation, key: BobKey) -> Get {
        debug!(
            "Get[{}] from pearl backend. operation: {:?}",
            key, operation
        );
        let vdisks_groups = self.vdisks_groups.clone();
        let vdisk_group = vdisks_groups
            .iter()
            .find(|vd| vd.can_process_operation(&operation))
            .cloned();
        let task = async move {
            if let Some(group) = vdisk_group {
                let res = Self::get_common(group, key).await;
                if let Err(e) = &res {
                    debug!("GET[{}], error: {:?}", key, e);
                }
                res
            } else {
                debug!(
                    "GET[{}] to pearl backend. Cannot find storage, operation: {:?}",
                    key, operation
                );
                Err(Error::VDiskNotFound(operation.vdisk_id()))
            }
        };
        task.boxed()
    }

    fn get_alien(&self, operation: BackendOperation, key: BobKey) -> Get {
        debug!("Get[alien][{}] from pearl backend", key);
        let backend = self.clone();
        let task = async move {
            let vdisk_group = backend.find_alien_pearl(operation.clone()).await;
            if let Ok(group) = vdisk_group {
                let res = Self::get_common(group, key).await;
                if let Err(e) = &res {
                    debug!("GET[alien][{}], error: {:?}", key, e);
                }
                res
            } else {
                debug!(
                    "GET[alien][{}] to pearl backend. Cannot find storage, operation: {:?}",
                    key, operation
                );
                Err(Error::KeyNotFound(key))
            }
        };
        task.boxed()
    }

    fn exist(&self, operation: BackendOperation, keys: &[BobKey]) -> Exist {
        let vdisk_group = self
            .vdisks_groups
            .iter()
            .find(|vd| vd.can_process_operation(&operation))
            .cloned();
        let keys = keys.to_vec();
        let task = async move {
            if let Some(group) = vdisk_group {
                Ok(group.exist(&keys).await)
            } else {
                Err(BackendError::Internal)
            }
        };
        task.boxed()
    }

    fn exist_alien(&self, operation: BackendOperation, keys: &[BobKey]) -> Exist {
        let backend = self.clone();
        let keys = keys.to_vec();
        let task = async move {
            let vdisk_group = backend.find_alien_pearl(operation.clone()).await;
            if let Ok(group) = vdisk_group {
                Ok(group.exist(&keys).await)
            } else {
                Err(BackendError::Internal)
            }
        };
        task.boxed()
    }

    fn vdisks_groups(&self) -> Option<&[Group]> {
        Some(&self.vdisks_groups)
    }
}
