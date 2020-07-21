use super::prelude::*;

pub(crate) type BackendResult<T> = std::result::Result<T, Error>;
pub(crate) type PearlStorage = Storage<Key>;

#[derive(Clone, Debug)]
pub(crate) struct Pearl {
    settings: Arc<Settings>,
    vdisks_groups: Arc<Vec<Group>>,
    alien_vdisks_groups: Arc<RwLock<Vec<Group>>>,
    pearl_sync: Arc<SyncState>, // holds state when we create new alien pearl dir
    node_name: String,
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
            node_name: config.name().to_string(),
        }
    }

    async fn create_alien_pearl(&self, operation: Operation) -> BackendResult<()> {
        // check if pearl is currently creating
        if self.pearl_sync.try_init().await {
            // check if alien created
            debug!("create alien for: {:?}", operation);
            if self.find_alien_pearl(operation.clone()).await.is_err() {
                let pearl = self
                    .settings
                    .clone()
                    .create_group(&operation, &self.node_name)
                    .expect("pearl group");
                let mut groups = self.alien_vdisks_groups.write().await;
                groups.push(pearl.clone());
            }
            self.pearl_sync.mark_as_created().await;
        }
        Ok(())
    }

    async fn find_alien_pearl(&self, operation: Operation) -> BackendResult<Group> {
        let operation = operation.clone();
        let pearls = self.alien_vdisks_groups.read().await;
        pearls
            .iter()
            .find(|group| group.can_process_operation(&operation))
            .cloned()
            .ok_or_else(|| {
                Error::failed(format!("cannot find actual alien folder. {:?}", operation))
            })
    }
}

#[async_trait]
impl BackendStorage for Pearl {
    async fn run_backend(&self) -> Result<(), Error> {
        debug!("run pearl backend");
        for vdisk_group in self.vdisks_groups.iter() {
            vdisk_group.run().await?;
        }
        let pearl_groups = self.alien_vdisks_groups.read().await;
        for group in pearl_groups.iter() {
            group.run().await?;
        }
        Ok(())
    }

    async fn put(&self, operation: Operation, key: BobKey, data: BobData) -> Result<(), Error> {
        debug!("PUT[{}] to pearl backend. opeartion: {:?}", key, operation);
        let vdisk_group = self
            .vdisks_groups
            .iter()
            .find(|vd| vd.can_process_operation(&operation));

        if let Some(group) = vdisk_group {
            let res = group.put(key, data).await;
            if let Err(e) = &res {
                debug!("PUT[{}], error: {:?}", key, e);
            }
            res
        } else {
            debug!(
                "PUT[{}] to pearl backend. Cannot find group, operation: {:?}",
                key, operation
            );
            Err(Error::vdisk_not_found(operation.vdisk_id()))
        }
    }

    async fn put_alien(
        &self,
        operation: Operation,
        key: BobKey,
        data: BobData,
    ) -> Result<(), Error> {
        debug!(
            "PUT[alien][{}] to pearl backend, operation: {:?}",
            key, operation
        );

        let mut vdisk_group = self.find_alien_pearl(operation.clone()).await;
        if vdisk_group.is_err() {
            debug!("need create alien for: {:?}", operation);
            self.create_alien_pearl(operation.clone())
                .await
                .expect("create alien pearl");
            vdisk_group = self.find_alien_pearl(operation.clone()).await;
        }
        if let Ok(group) = vdisk_group {
            let res = group.put(key, data).await;
            if let Err(e) = &res {
                debug!("PUT[alien][{}], error: {:?}", key, e);
            }
            res
        } else {
            debug!(
                "PUT[alien][{}] to pearl backend. Cannot find group, operation: {:?}",
                key, operation
            );
            Err(Error::vdisk_not_found(operation.vdisk_id()))
        }
    }

    async fn get(&self, operation: Operation, key: BobKey) -> Result<BobData, Error> {
        debug!(
            "Get[{}] from pearl backend. operation: {:?}",
            key, operation
        );
        let vdisk_group = self
            .vdisks_groups
            .iter()
            .find(|vd| vd.can_process_operation(&operation));

        if let Some(group) = vdisk_group {
            let res = group.get(key).await;
            if let Err(e) = &res {
                debug!("GET[{}], error: {:?}", key, e);
            }
            res
        } else {
            debug!(
                "GET[{}] to pearl backend. Cannot find storage, operation: {:?}",
                key, operation
            );
            Err(Error::vdisk_not_found(operation.vdisk_id()))
        }
    }

    async fn get_alien(&self, operation: Operation, key: BobKey) -> Result<BobData, Error> {
        debug!("Get[alien][{}] from pearl backend", key);
        let vdisk_group = self.find_alien_pearl(operation.clone()).await;
        if let Ok(group) = vdisk_group {
            let res = group.get(key).await;
            if let Err(e) = &res {
                debug!("GET[alien][{}], error: {:?}", key, e);
            }
            res
        } else {
            debug!(
                "GET[alien][{}] to pearl backend. Cannot find storage, operation: {:?}",
                key, operation
            );
            Err(Error::key_not_found(key))
        }
    }

    async fn exist(&self, operation: Operation, keys: &[BobKey]) -> Result<Vec<bool>, Error> {
        let vdisk_group = self
            .vdisks_groups
            .iter()
            .find(|vd| vd.can_process_operation(&operation));
        if let Some(group) = vdisk_group {
            Ok(group.exist(&keys).await)
        } else {
            Err(Error::internal())
        }
    }

    async fn exist_alien(&self, operation: Operation, keys: &[BobKey]) -> Result<Vec<bool>, Error> {
        let vdisk_group = self.find_alien_pearl(operation.clone()).await;
        if let Ok(group) = vdisk_group {
            Ok(group.exist(&keys).await)
        } else {
            Err(Error::internal())
        }
    }

    fn vdisks_groups(&self) -> Option<&[Group]> {
        Some(&self.vdisks_groups)
    }
}
