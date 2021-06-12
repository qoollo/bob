use tokio::time::{interval, Interval};

pub(crate) mod logger;
use crate::{core::Operation, prelude::*};
use logger::DisksEventsLogger;

use super::{core::BackendResult, settings::Settings, Group};

use bob_common::metrics::DISKS_FOLDER;

const CHECK_INTERVAL: Duration = Duration::from_millis(5000);

const DISK_IS_NOT_ACTIVE: i64 = 0;
const DISK_IS_ACTIVE: i64 = 1;

#[derive(Clone, Debug, PartialEq)]
enum GroupsState {
    // group vector is empty or broken:
    // state before init OR after disk becomes unavailable
    NotReady = 0,
    // groups are read from disk but don't run
    Initialized = 2,
    // groups are ready to process request
    Ready = 3,
}

#[derive(Clone, Debug)]
pub struct DiskController {
    disk: DiskPath,
    vdisks: Vec<VDiskId>,
    dump_sem: Arc<Semaphore>,
    run_sem: Arc<Semaphore>,
    monitor_sem: Arc<Semaphore>,
    node_name: String,
    groups: Arc<RwLock<Vec<Group>>>,
    state: Arc<RwLock<GroupsState>>,
    settings: Arc<Settings>,
    is_alien: bool,
    disk_state_metric: String,
    logger: DisksEventsLogger,
}

impl DiskController {
    pub(crate) async fn new(
        disk: DiskPath,
        vdisks: Vec<VDiskId>,
        config: &NodeConfig,
        run_sem: Arc<Semaphore>,
        settings: Arc<Settings>,
        is_alien: bool,
        logger: DisksEventsLogger,
    ) -> Arc<Self> {
        let disk_state_metric = format!("{}.{}", DISKS_FOLDER, disk.name());
        let dump_sem = Arc::new(Semaphore::new(config.disk_access_par_degree()));
        let new_dc = Self {
            disk,
            vdisks,
            dump_sem,
            run_sem,
            monitor_sem: Arc::new(Semaphore::new(1)),
            node_name: config.name().to_owned(),
            groups: Arc::new(RwLock::new(Vec::new())),
            state: Arc::new(RwLock::new(GroupsState::NotReady)),
            settings,
            is_alien,
            disk_state_metric,
            logger,
        };
        new_dc
            .init()
            .await
            .expect("Can't start new disk controller");
        let new_dc = Arc::new(new_dc);
        let cloned_dc = new_dc.clone();
        tokio::spawn(async move { cloned_dc.monitor_task().await });
        new_dc
    }

    fn is_work_dir_available(path: impl AsRef<Path>) -> bool {
        path.as_ref().exists()
    }

    pub async fn is_ready(&self) -> bool {
        *self.state.read().await == GroupsState::Ready
    }

    pub async fn run(&self) -> AnyResult<()> {
        let _permit = self.monitor_sem.acquire().await.expect("Sem is closed");
        self.init().await?;
        self.groups_run().await
    }

    pub async fn stop(&self) {
        let _permit = self.monitor_sem.acquire().await.expect("Sem is closed");
        self.change_state(GroupsState::NotReady).await;
    }

    async fn monitor_wait(state: Arc<RwLock<GroupsState>>, check_interval: &mut Interval) {
        while *state.read().await != GroupsState::Ready {
            check_interval.tick().await;
        }
    }

    async fn monitor_task(self: Arc<Self>) {
        let mut check_interval = interval(CHECK_INTERVAL);
        Self::monitor_wait(self.state.clone(), &mut check_interval).await;
        loop {
            check_interval.tick().await;
            let _permit = self.monitor_sem.acquire().await.expect("Sem is closed");
            if Self::is_work_dir_available(self.disk.path()) {
                let state = self.state.read().await.clone();
                match state {
                    GroupsState::NotReady | GroupsState::Initialized => {
                        info!(
                            "Work dir is available, but disk is not running ({:?})",
                            self.disk
                        );
                    }
                    GroupsState::Ready => {
                        info!("Disk is available: {:?}", self.disk);
                    }
                }
            } else {
                error!("Disk is unavailable: {:?}", self.disk);
                self.change_state(GroupsState::NotReady).await;
            }
        }
    }

    async fn change_state(&self, new_state: GroupsState) {
        let mut state_wlock = self.state.write().await;
        if *state_wlock == new_state {
            debug!("Identity transformation");
            return;
        }
        match (&*state_wlock, &new_state) {
            (_, GroupsState::NotReady) => {
                self.log_state_change(&new_state).await;
                // if disk is broken (either are indices in groups) we should drop groups, because
                // otherwise we'll hold broken indices (for active blob of broken disk) in RAM
                self.shutdown().await;
                self.groups.write().await.clear();
            }
            (GroupsState::NotReady, GroupsState::Initialized) => {
                self.log_state_change(&new_state).await;
            }
            (GroupsState::Initialized, GroupsState::Ready) => {
                self.log_state_change(&new_state).await
            }
            _ => error!("Invalid transformation"),
        }
        *state_wlock = new_state;
    }

    // on pearl level only write operations can map OS errors into work_dir error, so this
    // processing for error is done only for put operations
    async fn process_error(&self, e: Error) -> Error {
        // FIXME: Do we need to validate somehow that storage is really not accessible?
        if e.is_possible_disk_disconnection() {
            self.change_state(GroupsState::NotReady).await;
        }
        e
    }

    async fn log_state_change(&self, new_state: &GroupsState) {
        match new_state {
            GroupsState::NotReady => {
                gauge!(self.disk_state_metric.clone(), DISK_IS_NOT_ACTIVE);
                self.logger
                    .log(self.disk().name(), "off", self.is_alien)
                    .await;
                error!("Disk is not ready: {:?}", self.disk);
            }
            GroupsState::Initialized => {
                warn!("Disk is initialized: {:?}", self.disk);
            }
            GroupsState::Ready => {
                gauge!(self.disk_state_metric.clone(), DISK_IS_ACTIVE);
                self.logger
                    .log(self.disk().name(), "on", self.is_alien)
                    .await;
                warn!("Disk is ready: {:?}", self.disk);
            }
        }
    }

    async fn init(&self) -> BackendResult<()> {
        let state = self.state.read().await.clone();
        if state == GroupsState::NotReady {
            let groups = self.get_groups().await?;
            *self.groups.write().await = groups;
            self.change_state(GroupsState::Initialized).await;
        }
        Ok(())
    }

    async fn get_groups(&self) -> BackendResult<Vec<Group>> {
        if self.is_alien {
            self.collect_alien_groups().await
        } else {
            Ok(self.collect_normal_groups())
        }
    }

    pub fn disk(&self) -> &DiskPath {
        &self.disk
    }

    pub fn vdisks(&self) -> &[VDiskId] {
        &self.vdisks
    }

    pub async fn vdisk_group(&self, vdisk_id: VDiskId) -> AnyResult<Group> {
        let groups = self.groups.read().await;
        let group_opt = groups.iter().find(|g| g.vdisk_id() == vdisk_id).cloned();
        group_opt.ok_or_else(|| Error::vdisk_not_found(vdisk_id).into())
    }

    fn collect_normal_groups(&self) -> Vec<Group> {
        self.vdisks
            .iter()
            .copied()
            .map(|vdisk_id| {
                let path = self.settings.normal_path(self.disk.path(), vdisk_id);
                Group::new(
                    self.settings.clone(),
                    vdisk_id,
                    self.node_name.clone(),
                    self.disk.name().to_owned(),
                    path,
                    self.node_name.clone(),
                    self.dump_sem.clone(),
                )
            })
            .collect()
    }

    async fn collect_alien_groups(&self) -> BackendResult<Vec<Group>> {
        let settings = self.settings.clone();
        let groups = settings
            .collect_alien_groups(self.disk.name().to_owned(), self.dump_sem.clone())
            .await?;
        trace!(
            "alien vdisk groups count (start or recovery): {}",
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
            .find(|group| group.can_process_operation(operation))
            .cloned()
    }

    async fn get_or_create_pearl(&self, operation: &Operation) -> BackendResult<Group> {
        trace!("try get alien pearl, operation {:?}", operation);
        // block for read lock: it should be dropped before write lock is acquired (otherwise - deadlock)
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

        let group = self
            .settings
            .clone()
            .create_group(operation, &self.node_name, self.dump_sem.clone())
            .await?;
        write_lock_groups.push(group.clone());
        Ok(group)
    }

    async fn run_groups(groups: Arc<RwLock<Vec<Group>>>) -> AnyResult<()> {
        for group in groups.read().await.iter() {
            group.run().await?;
        }
        Ok(())
    }

    async fn groups_run(&self) -> AnyResult<()> {
        let state = self.state.read().await.clone();
        match state {
            GroupsState::Initialized => self.groups_run_initialized().await,
            GroupsState::Ready => Ok(()),
            GroupsState::NotReady => Err(Error::internal().into()),
        }
    }

    async fn groups_run_initialized(&self) -> AnyResult<()> {
        let res = {
            let _permit = self.run_sem.acquire().await.expect("Semaphore is closed");
            Self::run_groups(self.groups.clone()).await
        };
        if let Err(e) = &res {
            error!("Can't run groups on disk {:?} (reason: {})", self.disk, e);
            if !Self::is_work_dir_available(self.disk.path()) {
                self.change_state(GroupsState::NotReady).await;
            }
        } else {
            self.change_state(GroupsState::Ready).await;
            info!("All groups are running on disk: {:?}", self.disk);
        }
        res
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
                Ok(group) => match group.put(key, data.clone()).await {
                    Err(e) => Err(self.process_error(e).await),
                    Ok(()) => Ok(()),
                },
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
            let vdisk_group = {
                let groups = self.groups.read().await;
                groups
                    .iter()
                    .find(|vd| vd.can_process_operation(&op))
                    .cloned()
            };
            if let Some(group) = vdisk_group {
                match group.put(key, data).await {
                    Err(e) => {
                        debug!("PUT[{}], error: {:?}", key, e);
                        Err(self.process_error(e).await)
                    }
                    Ok(()) => Ok(()),
                }
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
                Ok(group.exist(keys).await)
            } else {
                Err(Error::internal())
            }
        } else {
            Err(Error::dc_is_not_available())
        }
    }

    pub(crate) async fn shutdown(&self) {
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
        futures.collect::<()>().await;
    }

    pub(crate) async fn blobs_count(&self) -> usize {
        if *self.state.read().await == GroupsState::Ready {
            let mut cnt = 0;
            for group in self.groups.read().await.iter() {
                let holders_guard = group.holders();
                let holders = holders_guard.read().await;
                for holder in holders.iter() {
                    cnt += holder.blobs_count().await;
                }
            }
            cnt
        } else {
            0
        }
    }

    pub(crate) async fn index_memory(&self) -> usize {
        if *self.state.read().await == GroupsState::Ready {
            let mut cnt = 0;
            for group in self.groups.read().await.iter() {
                let holders_guard = group.holders();
                let holders = holders_guard.read().await;
                for holder in holders.iter() {
                    cnt += holder.index_memory().await;
                }
            }
            cnt
        } else {
            0
        }
    }

    pub(crate) async fn close_unneeded_active_blobs(&self, soft: usize, hard: usize) {
        let groups = self.groups.read().await;
        for group in groups.iter() {
            group.close_unneeded_active_blobs(soft, hard).await;
        }
    }
}
