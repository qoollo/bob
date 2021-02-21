use super::prelude::*;

#[derive(Clone, Debug)]
pub(crate) struct DiskController {
    disk: DiskPath,
    vdisks: Vec<VDiskID>,
    init_par_degree: usize,
    node_name: String,
    // groups may change (and disk controller is responsible for that)
    groups: Arc<RwLock<Vec<Group>>>,
    settings: Arc<Settings>,
    is_alien: bool,
}

impl DiskController {
    pub(crate) fn new(
        disk: DiskPath,
        vdisks: Vec<VDiskID>,
        config: &NodeConfig,
        settings: Arc<Settings>,
        is_alien: bool,
    ) -> Self {
        Self {
            disk,
            vdisks,
            init_par_degree: config.init_par_degree(),
            node_name: config.name().to_owned(),
            groups: Arc::new(RwLock::new(Vec::new())),
            settings,
            is_alien,
        }
    }

    pub(crate) fn init(&mut self) -> BackendResult<()> {
        if self.is_alien {
            self.init_alien_groups()?;
        } else {
            self.init_groups();
        }
        Ok(())
    }

    pub(crate) fn init_groups(&mut self) {
        let groups = self
            .vdisks
            .iter()
            .copied()
            .map(|vdisk_id| {
                let dump_sem = Arc::new(Semaphore::new(self.init_par_degree));
                let path = self.settings.normal_path(self.disk.path(), vdisk_id);
                Group::new(
                    self.settings.clone(),
                    vdisk_id,
                    self.node_name.clone(),
                    self.disk.name().to_owned(),
                    path,
                    self.node_name.clone(),
                    dump_sem.clone(),
                )
            })
            .collect();
        self.groups = Arc::new(RwLock::new(groups));
    }

    pub(crate) fn init_alien_groups(&mut self) -> BackendResult<()> {
        let groups = self
            .settings
            .clone()
            .collect_alien_groups(self.disk.name().to_owned())?;
        trace!(
            "count alien vdisk groups (start or recovery): {}",
            groups.len()
        );
        self.groups = Arc::new(RwLock::new(groups));
        Ok(())
    }

    async fn find_group(&self, operation: &Operation) -> BackendResult<Group> {
        Self::find_in_groups(self.groups.read().await.iter(), operation).ok_or_else(|| {
            Error::failed(format!("cannot find actual alien folder. {:?}", operation))
        })
    }

    fn find_in_groups<'pearl, 'op>(
        mut pearls: impl Iterator<Item = &'pearl Group>,
        operation: &'op Operation,
    ) -> Option<Group> {
        pearls
            .find(|group| group.can_process_operation(&operation))
            .cloned()
    }

    async fn get_or_create_pearl(&self, operation: &Operation) -> BackendResult<Group> {
        trace!("try get alien pearl, operation {:?}", operation);
        {
            let read_lock_groups = self.groups.read().await;
            let pearl = Self::find_in_groups(read_lock_groups.iter(), operation);
            if let Some(g) = pearl {
                return Ok(g);
            }
        }

        let mut write_lock_groups = self.groups.write().await;
        let pearl = Self::find_in_groups(write_lock_groups.iter(), operation);
        if let Some(g) = pearl {
            return Ok(g);
        }

        self.settings
            .clone()
            .create_group(operation, &self.node_name)
            .map(|g| {
                write_lock_groups.push(g.clone());
                g
            })
    }

    pub(crate) async fn run(&self) -> Result<()> {
        for group in self.groups.read().await.iter() {
            group.run().await?;
        }
        Ok(())
    }

    pub(crate) async fn put_alien(
        &self,
        op: Operation,
        key: BobKey,
        data: BobData,
    ) -> Result<(), Error> {
        let vdisk_group = self.get_or_create_pearl(&op).await;
        match vdisk_group {
            Ok(group) => {
                let res = group.put(key, data.clone()).await;
                res.map_err(|e| Error::failed(format!("{:#?}", e)))
            }
            Err(e) => {
                error!(
                    "PUT[alien][{}] Cannot find group, op: {:?}, err: {}",
                    key, op, e
                );
                Err(Error::vdisk_not_found(op.vdisk_id()))
            }
        }
    }

    pub(crate) async fn put(&self, op: Operation, key: BobKey, data: BobData) -> BackendResult<()> {
        let vdisk_group = self
            .groups
            .read()
            .await
            .iter()
            .find(|vd| vd.can_process_operation(&op))
            .cloned();
        if let Some(group) = vdisk_group {
            let res = group.put(key, data).await;
            res.map_err(|e| {
                debug!("PUT[{}], error: {:?}", key, e);
                Error::failed(format!("{:#?}", e))
            })
        } else {
            debug!("PUT[{}] Cannot find group, operation: {:?}", key, op);
            Err(Error::vdisk_not_found(op.vdisk_id()))
        }
    }

    pub(crate) fn can_process_operation(&self, op: &Operation) -> bool {
        self.is_alien && op.is_data_alien()
            || self
                .vdisks
                .iter()
                .any(|&vdisk_id| vdisk_id == op.vdisk_id())
    }

    pub(crate) async fn get(&self, op: Operation, key: BobKey) -> Result<BobData, Error> {
        debug!("Get[{}] from pearl backend. operation: {:?}", key, op);
        let vdisk_group = self
            .groups
            .read()
            .await
            .iter()
            .find(|g| g.can_process_operation(&op))
            .cloned();
        if let Some(group) = vdisk_group {
            group.get(key).await
        } else {
            error!("GET[{}] Cannot find storage, operation: {:?}", key, op);
            Err(Error::vdisk_not_found(op.vdisk_id()))
        }
    }

    pub(crate) async fn get_alien(&self, op: Operation, key: BobKey) -> Result<BobData, Error> {
        let vdisk_group = self.find_group(&op).await;
        if let Ok(group) = vdisk_group {
            group.get(key).await
        } else {
            warn!(
                "GET[alien][{}] No alien group has been created for vdisk #{}",
                key,
                op.vdisk_id()
            );
            Err(Error::key_not_found(key))
        }
    }

    pub(crate) async fn exist(
        &self,
        operation: Operation,
        keys: &[BobKey],
    ) -> Result<Vec<bool>, Error> {
        let group_option = self
            .groups
            .read()
            .await
            .iter()
            .find(|g| g.can_process_operation(&operation))
            .cloned();
        if let Some(group) = group_option {
            Ok(group.exist(&keys).await)
        } else {
            Err(Error::internal())
        }
    }

    pub(crate) async fn shutdown(&self) {
        //use futures::stream::FuturesUnordered;
        let futures = FuturesUnordered::new();
        for group in self.groups.read().await.iter() {
            let holders = group.holders();
            let holders = holders.read().await;
            for holder in holders.iter() {
                let storage = holder.storage().read().await;
                let storage = storage.storage().clone();
                let id = holder.get_id();
                futures.push(async move {
                    match storage.close().await {
                        Ok(_) => debug!("holder {} closed", id),
                        Err(e) => error!("error closing holder{}: {}", id, e),
                    }
                });
            }
        }
        let _ = futures.collect::<()>().await;
    }

    pub(crate) async fn blobs_count(&self) -> usize {
        let mut cnt = 0;
        for group in self.groups.read().await.iter() {
            let holders_guard = group.holders();
            let holders = holders_guard.read().await;
            for holder in holders.iter() {
                cnt += holder.blobs_count().await;
            }
        }
        cnt
    }

    pub(crate) async fn index_memory(&self) -> usize {
        let mut cnt = 0;
        for group in self.groups.read().await.iter() {
            let holders_guard = group.holders();
            let holders = holders_guard.read().await;
            for holder in holders.iter() {
                cnt += holder.index_memory().await;
            }
        }
        cnt
    }
}
