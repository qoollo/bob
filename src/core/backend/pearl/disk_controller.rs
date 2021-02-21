use super::prelude::*;
use tokio::time::interval;

const CHECK_INTERVAL: Duration = Duration::from_millis(5000);

#[derive(Clone, Debug, PartialEq)]
enum GroupsState {
    // group vector is empty or broken:
    // state before init OR after disk becomes unavailable
    NotReady = 0,
    // groups are read from disk but don't run
    Initialized = 1,
    // groups are ready to process request
    Ready = 2,
}

#[derive(Clone, Debug)]
pub(crate) struct DiskController {
    disk: DiskPath,
    vdisks: Vec<VDiskID>,
    init_par_degree: usize,
    node_name: String,
    // groups may change (and disk controller is responsible for that)
    groups: Arc<RwLock<Vec<Group>>>,
    state: Arc<RwLock<GroupsState>>,
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
    ) -> Arc<Self> {
        let mut new_dc = Self {
            disk,
            vdisks,
            init_par_degree: config.init_par_degree(),
            node_name: config.name().to_owned(),
            groups: Arc::new(RwLock::new(Vec::new())),
            state: Arc::new(RwLock::new(GroupsState::NotReady)),
            settings,
            is_alien,
        };
        new_dc.sync_init().expect("Can't start new disk controller");
        let new_dc = Arc::new(new_dc);
        let cloned_dc = new_dc.clone();
        tokio::spawn(async move { cloned_dc.monitor_task().await });
        new_dc
    }

    fn is_work_dir_available(path: impl AsRef<Path>) -> bool {
        path.as_ref().exists()
    }

    async fn try_from_scratch(&self) -> Result<()> {
        self.init().await?;
        *self.state.write().await = GroupsState::Initialized;
        self.try_run().await
    }

    async fn try_run(&self) -> Result<()> {
        self.run().await?;
        *self.state.write().await = GroupsState::Ready;
        Ok(())
    }

    async fn monitor_task(self: Arc<Self>) {
        let mut check_interval = interval(CHECK_INTERVAL);
        loop {
            check_interval.tick().await;
            if !Self::is_work_dir_available(self.disk.path()) {
                error!("Disk is unavailable: {:?}", self.disk);
                *self.state.write().await = GroupsState::NotReady;
            } else {
                let state = self.state.read().await.clone();
                match state {
                    GroupsState::NotReady => {
                        if let Err(e) = self.try_from_scratch().await {
                            warn!("Work dir is available, but initialization from scratch is failed: {}", e);
                        }
                    }
                    GroupsState::Initialized => {
                        if let Err(e) = self.try_run().await {
                            warn!("Work dir is available, but failed to run groups: {}", e);
                        }
                    }
                    GroupsState::Ready => {
                        // debug?
                        info!("Disk is available: {:?}", self.disk);
                    }
                }
            }
        }
    }

    // this init is done in `new` sync method. Monitor use async init.
    pub(crate) fn sync_init(&mut self) -> BackendResult<()> {
        self.groups = Arc::new(RwLock::new(self.get_groups()?));
        self.state = Arc::new(RwLock::new(GroupsState::Initialized));
        Ok(())
    }

    pub(crate) async fn init(&self) -> BackendResult<()> {
        let groups = self.get_groups()?;
        *self.groups.write().await = groups;
        *self.state.write().await = GroupsState::Initialized;
        Ok(())
    }

    fn get_groups(&self) -> BackendResult<Vec<Group>> {
        if self.is_alien {
            self.collect_alien_groups()
        } else {
            Ok(self.collect_normal_groups())
        }
    }

    pub(crate) fn collect_normal_groups(&self) -> Vec<Group> {
        self.vdisks
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
            .collect()
    }

    pub(crate) fn collect_alien_groups(&self) -> BackendResult<Vec<Group>> {
        let groups = self
            .settings
            .clone()
            .collect_alien_groups(self.disk.name().to_owned())?;
        trace!(
            "count alien vdisk groups (start or recovery): {}",
            groups.len()
        );
        Ok(groups)
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

    async fn run_groups(groups: Arc<RwLock<Vec<Group>>>) -> Result<()> {
        for group in groups.read().await.iter() {
            group.run().await?;
        }
        Ok(())
    }

    pub(crate) async fn run(&self) -> Result<()> {
        Self::run_groups(self.groups.clone()).await?;
        *self.state.write().await = GroupsState::Ready;
        info!("All groups are sucessfully on disk: {:?}", self.disk);
        Ok(())
    }

    pub(crate) async fn put_alien(
        &self,
        op: Operation,
        key: BobKey,
        data: BobData,
    ) -> Result<(), Error> {
        if *self.state.read().await == GroupsState::Ready {
            let vdisk_group = self.get_or_create_pearl(&op).await;
            match vdisk_group {
                Ok(group) => {
                    // TODO: check if pearl WorkDirUnavailable error occured and change state
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
        } else {
            Err(Error::dc_is_not_available())
        }
    }

    pub(crate) async fn put(&self, op: Operation, key: BobKey, data: BobData) -> BackendResult<()> {
        if *self.state.read().await == GroupsState::Ready {
            let vdisk_group = self
                .groups
                .read()
                .await
                .iter()
                .find(|vd| vd.can_process_operation(&op))
                .cloned();
            if let Some(group) = vdisk_group {
                let res = group.put(key, data).await;
                // TODO: check if pearl WorkDirUnavailable error occured and change state
                res.map_err(|e| {
                    debug!("PUT[{}], error: {:?}", key, e);
                    Error::failed(format!("{:#?}", e))
                })
            } else {
                debug!("PUT[{}] Cannot find group, operation: {:?}", key, op);
                Err(Error::vdisk_not_found(op.vdisk_id()))
            }
        } else {
            Err(Error::dc_is_not_available())
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
        if *self.state.read().await == GroupsState::Ready {
            debug!("Get[{}] from pearl backend. operation: {:?}", key, op);
            let vdisk_group = self
                .groups
                .read()
                .await
                .iter()
                .find(|g| g.can_process_operation(&op))
                .cloned();
            if let Some(group) = vdisk_group {
                // TODO: check if pearl WorkDirUnavailable error occured and change state
                group.get(key).await
            } else {
                error!("GET[{}] Cannot find storage, operation: {:?}", key, op);
                Err(Error::vdisk_not_found(op.vdisk_id()))
            }
        } else {
            Err(Error::dc_is_not_available())
        }
    }

    pub(crate) async fn get_alien(&self, op: Operation, key: BobKey) -> Result<BobData, Error> {
        if *self.state.read().await == GroupsState::Ready {
            let vdisk_group = self.find_group(&op).await;
            if let Ok(group) = vdisk_group {
                // TODO: check if pearl WorkDirUnavailable error occured and change state
                group.get(key).await
            } else {
                warn!(
                    "GET[alien][{}] No alien group has been created for vdisk #{}",
                    key,
                    op.vdisk_id()
                );
                Err(Error::key_not_found(key))
            }
        } else {
            Err(Error::dc_is_not_available())
        }
    }

    pub(crate) async fn exist(
        &self,
        operation: Operation,
        keys: &[BobKey],
    ) -> Result<Vec<bool>, Error> {
        if *self.state.read().await == GroupsState::Ready {
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
        } else {
            Err(Error::dc_is_not_available())
        }
    }

    pub(crate) async fn shutdown(&self) {
        // TODO: work somehow according to the state
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
                        Err(e) => {
                            error!("error closing holder{}: {} (disk: {:?})", id, e, self.disk)
                        }
                    }
                });
            }
        }
        let _ = futures.collect::<()>().await;
    }

    pub(crate) async fn blobs_count(&self) -> usize {
        // TODO: work somehow according to the state
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
        // TODO: work somehow according to the state
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
