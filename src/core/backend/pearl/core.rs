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

    async fn create_alien_pearl(&self, op: &Operation) -> BackendResult<()> {
        // check if pearl is currently creating
        if self.pearl_sync.is_ready().await {
            // check if alien created
            debug!("create alien for: {:?}", op);
            if !self.has_ready_alien_pearl(&op).await {
                let group = self
                    .settings
                    .clone()
                    .create_group(op, &self.node_name)
                    .expect("pearl group");
                let mut groups = self.alien_vdisks_groups.write().await;
                groups.push(group);
            }
            self.pearl_sync.mark_as_created().await;
        }
        Ok(())
    }

    async fn has_ready_alien_pearl(&self, op: &Operation) -> bool {
        self.alien_vdisks_groups
            .read()
            .await
            .iter()
            .any(|group| group.can_process_operation(&op))
    }

    async fn find_alien_pearl(&self, operation: &Operation) -> BackendResult<Group> {
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
    async fn run_backend(&self) -> Result<()> {
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

    async fn put(&self, op: Operation, key: BobKey, data: BobData) -> Result<(), Error> {
        debug!("PUT[{}] to pearl backend. opeartion: {:?}", key, op);
        let vdisk_group = self
            .vdisks_groups
            .iter()
            .find(|vd| vd.can_process_operation(&op));

        if let Some(group) = vdisk_group {
            let res = group.put(key, data).await;
            if let Err(e) = &res {
                debug!("PUT[{}], error: {:?}", key, e);
            }
            res.map_err(|e| Error::failed(format!("{:#?}", e)))
        } else {
            debug!("PUT[{}] Cannot find group, operation: {:?}", key, op);
            Err(Error::vdisk_not_found(op.vdisk_id()))
        }
    }

    async fn put_alien(&self, op: Operation, key: BobKey, data: BobData) -> Result<(), Error> {
        debug!("PUT[alien][{}] to pearl backend, operation: {:?}", key, op);

        if !self.has_ready_alien_pearl(&op).await {
            debug!("need to create alien for: {:?}", op);
            self.create_alien_pearl(&op).await?;
        }
        let vdisk_group = self.find_alien_pearl(&op).await;
        match vdisk_group {
            Ok(group) => group
                .put(key, data)
                .await
                .map_err(|e| Error::failed(format!("{:#?}", e))),
            Err(e) => {
                error!(
                    "PUT[alien][{}] Cannot find group, op: {:?}, err: {}",
                    key, op, e
                );
                Err(Error::vdisk_not_found(op.vdisk_id()))
            }
        }
    }

    async fn get(&self, op: Operation, key: BobKey) -> Result<BobData, Error> {
        debug!("Get[{}] from pearl backend. operation: {:?}", key, op);
        let vdisk_group = self
            .vdisks_groups
            .iter()
            .find(|vd| vd.can_process_operation(&op));

        if let Some(group) = vdisk_group {
            group.get(key).await
        } else {
            error!("GET[{}] Cannot find storage, operation: {:?}", key, op);
            Err(Error::vdisk_not_found(op.vdisk_id()))
        }
    }

    async fn get_alien(&self, op: Operation, key: BobKey) -> Result<BobData, Error> {
        debug!("Get[alien][{}] from pearl backend", key);
        let vdisk_group = self.find_alien_pearl(&op).await;
        if let Ok(group) = vdisk_group {
            group.get(key).await
        } else {
            error!("GET[alien][{}] Cannot find storage, op: {:?}", key, op);
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
        let vdisk_group = self.find_alien_pearl(&operation).await;
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
