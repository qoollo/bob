use super::{
    data::BackendResult,
    holder::PearlHolder,
    settings::Settings,
    stuff::{Stuff, SyncState},
};
use crate::core::{
    backend,
    backend::core::*,
    configs::node::PearlConfig,
    data::{BobData, BobKey, VDiskId},
};
use futures03::{compat::Future01CompatExt, task::Spawn, FutureExt};

use futures_locks::RwLock;
use std::{path::PathBuf, sync::Arc};

/// Wrap pearl holder and add timestamp info
#[derive(Clone)]
pub(crate) struct PearlTimestampHolder<TSpawner> {
    pub pearl: PearlHolder<TSpawner>,
    pub start_timestamp: i64,
    pub end_timestamp: i64,
} //TODO add path and fix Display

impl<TSpawner: Spawn + Clone + Send + 'static + Unpin + Sync> PearlTimestampHolder<TSpawner> {
    pub(crate) fn new(
        pearl: PearlHolder<TSpawner>,
        start_timestamp: i64,
        end_timestamp: i64,
    ) -> Self {
        PearlTimestampHolder {
            pearl,
            start_timestamp,
            end_timestamp,
        }
    }
}

impl<TSpawner> std::fmt::Display for PearlTimestampHolder<TSpawner> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.start_timestamp)
    }
}

/// Composition of pearls. Add put/get api
#[derive(Clone)]
pub(crate) struct PearlGroup<TSpawner> {
    /// all pearls
    pearls: Arc<RwLock<Vec<PearlTimestampHolder<TSpawner>>>>,
    // holds state when we create new pearl
    pearl_sync: Arc<SyncState>,

    settings: Arc<Settings<TSpawner>>,
    config: PearlConfig,
    spawner: TSpawner,

    vdisk_id: VDiskId,
    node_name: String,
    pub directory_path: PathBuf,
    disk_name: String,
}

impl<TSpawner: Spawn + Clone + Send + 'static + Unpin + Sync> PearlGroup<TSpawner> {
    pub fn new(
        settings: Arc<Settings<TSpawner>>,
        vdisk_id: VDiskId,
        node_name: String,
        disk_name: String,
        directory_path: PathBuf,
        config: PearlConfig,
        spawner: TSpawner,
    ) -> Self {
        PearlGroup {
            pearls: Arc::new(RwLock::new(vec![])),
            pearl_sync: Arc::new(SyncState::new()),
            settings,
            vdisk_id,
            node_name,
            directory_path,
            config,
            spawner,
            disk_name,
        }
    }

    pub fn can_process_operation(&self, operation: &BackendOperation) -> bool {
        if operation.is_data_alien() {
            operation.remote_node_name() == self.node_name && self.vdisk_id == operation.vdisk_id
        } else {
            self.disk_name == operation.disk_name_local() && self.vdisk_id == operation.vdisk_id
        }
    }

    pub async fn run(&self) {
        let delay = self.config.fail_retry_timeout();

        let mut pearls = vec![];

        let mut exit = false;

        debug!("{}: read pearls from disk", self);
        while !exit {
            let read_pearls = self.settings.read_vdisk_directory(self);
            if let Err(err) = read_pearls {
                error!("{}: can't create pearls: {:?}", self, err);
                let _ = Stuff::wait(delay).await;
            } else {
                pearls = read_pearls.unwrap();
                exit = true;
            }
        }
        debug!("{}: count pearls: {}", self, pearls.len());

        debug!("{}: check current pearl for write", self);
        if !pearls
            .iter()
            .any(|pearl| self.settings.is_actual_pearl(pearl))
        {
            let current_pearl = self.settings.create_current_pearl(self);
            debug!("{}: create new pearl: {}", self, current_pearl);
            pearls.push(current_pearl);
        }

        debug!("{}: save pearls to group", self);
        while let Err(err) = self.add_range(pearls.clone()).await {
            error!("{}: can't add pearls: {:?}", self, err);
            let _ = Stuff::wait(delay).await;
        }

        debug!("{}: start pearls", self);
        while let Err(err) = self.run_pearls().await {
            error!("{}: can't start pearls: {:?}", self, err);
            let _ = Stuff::wait(delay).await;
        }
    }

    async fn run_pearls(&self) -> BackendResult<()> {
        let pearls = self.pearls.write().compat().boxed().await.map_err(|e| {
            error!("{}: cannot take lock: {:?}", self, e);
            backend::Error::Failed(format!("cannot take lock: {:?}", e))
        })?;

        for i in 0..pearls.len() {
            let _ = pearls[i]
                .pearl
                .clone()
                .prepare_storage() //TODO add Box?
                .await;
        }
        Ok(())
    }

    pub fn create_pearl_by_path(&self, path: PathBuf) -> PearlHolder<TSpawner> {
        PearlHolder::new(
            self.vdisk_id.clone(),
            path,
            self.config.clone(),
            self.spawner.clone(),
        )
    }
    pub async fn add(&self, pearl: PearlTimestampHolder<TSpawner>) -> BackendResult<()> {
        let mut pearls = self.pearls.write().compat().boxed().await.map_err(|e| {
            error!("cannot take lock: {:?}", e);
            backend::Error::Failed(format!("cannot take lock: {:?}", e))
        })?;

        pearls.push(pearl);

        Ok(())
    }

    pub async fn add_range(
        &self,
        mut new_pearls: Vec<PearlTimestampHolder<TSpawner>>,
    ) -> BackendResult<()> {
        let mut pearls = self.pearls.write().compat().boxed().await.map_err(|e| {
            error!("cannot take lock: {:?}", e);
            backend::Error::Failed(format!("cannot take lock: {:?}", e))
        })?;

        pearls.append(&mut new_pearls);

        Ok(())
    }

    /// find in all pearls actual pearl and try create new
    async fn try_get_current_pearl(
        &self,
        key: BobKey,
        data: BobData,
    ) -> BackendResult<PearlTimestampHolder<TSpawner>> {
        let pearl = self
            .find_current_pearl(key, data.clone())
            .await
            .map_err(|e| {
                debug!("cannot find pearl: {}", e);
                e
            });
        if pearl.is_err() {
            match self.create_current_pearl(key, data.clone()).await {
                Ok(_) => {
                    return self.find_current_pearl(key, data).await.map_err(|e| {
                        error!("cannot find pearl after creation: {}", e);
                        e
                    })
                }
                Err(e) => {
                    error!("cannot create pearl: {}", e);
                    return Err(e);
                }
            }
        }
        pearl
    }

    /// find in all pearls actual pearl
    async fn find_current_pearl(
        &self,
        key: BobKey,
        data: BobData,
    ) -> BackendResult<PearlTimestampHolder<TSpawner>> {
        let pearls = self.pearls.read().compat().boxed().await.map_err(|e| {
            error!("cannot take lock: {:?}", e);
            backend::Error::Failed(format!("cannot take lock: {:?}", e))
        })?;

        for pearl in pearls.iter().rev() {
            if self.settings.is_actual(pearl.clone(), key, data.clone()) {
                return Ok(pearl.clone());
            }
        }
        trace!(
            "cannot find actual pearl folder. key: {}, meta: {}",
            key,
            data.meta
        );
        return Err(backend::Error::Failed(format!(
            "cannot find actual pearl folder. key: {}, meta: {}",
            key, data.meta
        )));
    }
    /// create pearl for current write
    async fn create_current_pearl(&self, key: BobKey, data: BobData) -> BackendResult<()> {
        // check if pearl is currently creating
        trace!("{}, try create new pearl for: {}", self, data.clone());
        if self.pearl_sync.try_init().await? {
            // check if pearl created
            debug!("{}, create new pearl for: {}", self, data.clone());
            if self.find_current_pearl(key, data.clone()).await.is_ok() {
                debug!("{}, find new pearl for: {}", self, data.clone());
                let _ = self.pearl_sync.mark_as_created().await;
                return Ok(());
            }
            let pearl = self.settings.create_current_pearl(self);
            debug!("{}, create new pearl: {} for: {}", self, pearl, data);

            let _ = self.save_pearl(pearl.clone()).await;
            let _ = self.pearl_sync.mark_as_created().await?;
        } else {
            let delay = self.config.settings().create_pearl_wait_delay();
            let _ = Stuff::wait(delay).await;
        }
        Ok(())
    }

    async fn save_pearl(&self, pearl: PearlTimestampHolder<TSpawner>) -> BackendResult<()> {
        let _ = self.add(pearl.clone()).await?; // TODO while retry?
        let _ = pearl.pearl.prepare_storage().await;
        Ok(())
    }

    pub async fn put(&self, key: BobKey, data: BobData) -> PutResult {
        let pearl = self.try_get_current_pearl(key, data.clone()).await?;

        Self::put_common(pearl.pearl, key, data).await
    }

    async fn put_common(pearl: PearlHolder<TSpawner>, key: BobKey, data: BobData) -> PutResult {
        let result = pearl
            .write(key, Box::new(data))
            .map(|r| r.map(|_ok| BackendPutResult {}))
            .await;
        if backend::Error::is_put_error_need_restart(result.as_ref().err())
            && pearl.try_reinit().await.unwrap()
        {
            let _ = pearl.reinit_storage().await;
        }
        result
    }

    pub async fn get(&self, key: BobKey) -> GetResult {
        let pearls = self.pearls.read().compat().boxed().await.map_err(|e| {
            error!("cannot take lock: {:?}", e);
            backend::Error::Failed(format!("cannot take lock: {:?}", e))
        })?;

        let mut has_error = false;
        let mut results = vec![];
        for pearl in pearls.iter() {
            let get = Self::get_common(pearl.pearl.clone(), key).await;
            match get {
                Ok(data) => {
                    trace!("get data: {} from: {}", data, pearl);
                    results.push(data);
                }
                Err(err) => {
                    has_error = true;
                    debug!("get error: {}, from : {}", err, pearl);
                }
            }
        }
        if results.is_empty() {
            if has_error {
                debug!("cannot read from some pearls");
                return Err(backend::Error::Failed(
                    "cannot read from some pearls".to_string(),
                ));
            } else {
                return Err(backend::Error::KeyNotFound);
            }
        }
        self.settings.choose_data(results)
    }

    async fn get_common(pearl: PearlHolder<TSpawner>, key: BobKey) -> GetResult {
        let result = pearl
            .read(key)
            .map(|r| r.map(|data| BackendGetResult { data }))
            .await;
        if backend::Error::is_get_error_need_restart(result.as_ref().err())
            && pearl.try_reinit().await.unwrap()
        {
            let _ = pearl.reinit_storage().await;
        }
        result
    }
}

impl<TSpawner> std::fmt::Display for PearlGroup<TSpawner> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "[id: {}, node: {}, path: {:?}, disk: {}]",
            self.vdisk_id, self.node_name, self.directory_path, self.disk_name
        )
    }
}
