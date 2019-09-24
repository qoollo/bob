use super::{data::BackendResult, holder::PearlHolder, settings::Settings};
use crate::core::{
    backend,
    backend::core::*,
    configs::node::PearlConfig,
    data::{BobData, BobKey, VDiskId},
};
use futures03::{compat::Future01CompatExt, task::Spawn, FutureExt};
use futures_locks::RwLock;
use std::{path::PathBuf, sync::Arc};
use tokio_timer::sleep;

/// Wrap pearl holder and add timestamp info
#[derive(Clone)]
pub(crate) struct PearlTimestampHolder<TSpawner> {
    pub pearl: PearlHolder<TSpawner>,
    pub start_timestamp: u32,
    pub end_timestamp: u32,
} //TODO add path and fix Display

impl<TSpawner: Spawn + Clone + Send + 'static + Unpin + Sync> PearlTimestampHolder<TSpawner> {
    pub(crate) fn new(
        pearl: PearlHolder<TSpawner>,
        start_timestamp: u32,
        end_timestamp: u32,
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
            settings,
            vdisk_id,
            node_name,
            directory_path,
            config,
            spawner,
            disk_name,
        }
    }

    pub fn equal(&self, operation: &BackendOperation) -> bool {
        if operation.is_data_alien() {
            operation.remote_node_name() == self.node_name && self.vdisk_id == operation.vdisk_id
        } else {
            self.disk_name == operation.disk_name_local() && self.vdisk_id == operation.vdisk_id
        }
    }

    pub async fn run(&self) {
        let delay = self.config.fail_retry_timeout();
        let mut exit = false;

        let mut pearls = vec![];
        // read all pearls from disk
        while !exit {
            let read_pearls = self.settings.read_vdisk_directory(self);
            if let Err(err) = read_pearls {
                error!("can't create pearls: {:?}", err);
                let _ = sleep(delay).compat().boxed().await;
                continue;
            }

            pearls = read_pearls.unwrap();
            exit = true;
        }

        // check current pearl for write
        if pearls.iter().any(|pearl|self.settings.is_actual_pearl(pearl)) {
            let current_pearl = self.settings.create_current_pearl(self);
            pearls.push(current_pearl);
        }

        exit = false;
        // save pearls to group
        while !exit {
            if let Err(err) = self.add_range(pearls.clone()).await {
                error!("can't add pearls: {:?}", err);
                let _ = sleep(delay).compat().boxed().await;
                continue;
            }
            exit = true;
        }

        exit = false;
        // start pearls
        while !exit {
            if let Err(err) = self.run_pearls().await {
                error!("can't start pearls: {:?}", err);
                let _ = sleep(delay).compat().boxed().await;
                continue;
            }
            exit = true;
        }
    }

    async fn run_pearls(&self) -> BackendResult<()> {
        let pearls = self.pearls.write().compat().boxed().await.map_err(|e| {
            error!("cannot take lock: {:?}", e);
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

    /// find in all pearls actual pearl
    fn get_actual(
        &self,
        list: Vec<PearlTimestampHolder<TSpawner>>,
        key: BobKey,
        data: BobData,
    ) -> BackendResult<PearlTimestampHolder<TSpawner>> {
        let mut i = list.len() - 1;
        while i >= 0 {
            if self.settings.is_actual(list[i].clone(), key, data.clone())
            //TODO add pointer to remove clonning
            {
                return Ok(list[i].clone());
            }
            i -= 1;
        }
        error!(
            "cannot find actual pearl folder. key: {}, meta: {}",
            key, data.meta
        );
        return Err(backend::Error::Failed(format!(
            "cannot find actual pearl folder. key: {}, meta: {}",
            key, data.meta
        )));
    }

    pub async fn put(&self, key: BobKey, data: BobData) -> PutResult {
        let pearls = self.pearls.read().compat().boxed().await.map_err(|e| {
            error!("cannot take lock: {:?}", e);
            backend::Error::Failed(format!("cannot take lock: {:?}", e))
        })?;

        let pearl = self.get_actual(pearls.to_vec(), key, data.clone())?;

        Self::put_common(pearl.pearl, key, data).await
    }

    fn is_write_error(err: Option<&backend::Error>) -> bool {
        match err {
            Some(backend::Error::DuplicateKey) | Some(backend::Error::VDiskIsNotReady) => false,
            Some(_) => true,
            _ => false,
        }
    }

    async fn put_common(pearl: PearlHolder<TSpawner>, key: BobKey, data: BobData) -> PutResult {
        let result = pearl
            .write(key, Box::new(data))
            .map(|r| r.map(|_ok| BackendPutResult {}))
            .await;
        if Self::is_write_error(result.as_ref().err()) && pearl.try_reinit().await.unwrap() {
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

    fn is_read_error(err: Option<&backend::Error>) -> bool {
        match err {
            Some(backend::Error::KeyNotFound) | Some(backend::Error::VDiskIsNotReady) => false,
            Some(_) => true,
            _ => false,
        }
    }

    async fn get_common(pearl: PearlHolder<TSpawner>, key: BobKey) -> GetResult {
        let result = pearl
            .read(key)
            .map(|r| r.map(|data| BackendGetResult { data }))
            .await;
        if Self::is_read_error(result.as_ref().err()) && pearl.try_reinit().await.unwrap() {
            let _ = pearl.reinit_storage().await;
        }
        result
    }
}
