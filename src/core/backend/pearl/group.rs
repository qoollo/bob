use super::{
    holder::PearlHolder,
    data::BackendResult,
    settings::Settings,
    stuff::*
    };
use crate::core::{
    backend,
    data::{BobData, BobKey, VDiskId},
    configs::node::PearlConfig,
    backend::core::*,
    };
use std::{sync::Arc, path::PathBuf, fs::read_dir};
use futures03::{
    compat::Future01CompatExt,
    task::Spawn,
    FutureExt,
};
use futures_locks::RwLock;
use tokio_timer::sleep;

#[derive(Clone)]
pub(crate) struct PearlTimestampHolder<TSpawner>{
    pub pearl: PearlHolder<TSpawner>,
    pub start_timestamp: u32,
    pub end_timestamp: u32,
}//TODO add path and fix Display

impl<TSpawner: Spawn + Clone + Send + 'static + Unpin + Sync> PearlTimestampHolder<TSpawner>{
    pub(crate) fn new(pearl: PearlHolder<TSpawner>, start_timestamp: u32, end_timestamp: u32) -> Self {
        PearlTimestampHolder{
            pearl, start_timestamp, end_timestamp,
        }
    }
}

impl<TSpawner> std::fmt::Display for PearlTimestampHolder<TSpawner> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.start_timestamp)
    }
}


pub(crate) struct PearlGroup<TSpawner> {
    group: Arc<RwLock<Vec<PearlTimestampHolder<TSpawner>>>>,
    settings: Arc<Settings>,
    config: PearlConfig, 
    spawner: TSpawner,
    
    vdisk_id: VDiskId,
    directory_path: PathBuf,
    disk_name: String,
}

impl<TSpawner: Spawn + Clone + Send + 'static + Unpin + Sync> PearlGroup<TSpawner> {
    pub fn new(settings: Arc<Settings>, vdisk_id: VDiskId, disk_name: String,  directory_path: PathBuf, config: PearlConfig, spawner: TSpawner) -> Self {
        PearlGroup {
            group: Arc::new(RwLock::new(vec![])),
            settings,
            vdisk_id,
            directory_path,
            config,
            spawner,
            disk_name,
        }
    }

    pub async fn run(&self) {
        let delay = self.config.fail_retry_timeout();
        let mut exit = false;

        let mut pearls = vec![];
        while !exit {
            let read_pearls = self.read_vdisk_directory();
            if let Err(err) = read_pearls {
                error!("can't create pearls: {:?}", err);
                let _ = sleep(delay).compat().boxed().await;
                continue;
            }

            pearls = read_pearls.unwrap();
            exit = true;
        }

        exit = false;
        while !exit {
            if let Err(err) = self.add_range(pearls.clone()).await {
                error!("can't add pearls: {:?}", err);
                let _ = sleep(delay).compat().boxed().await;
                continue;
            }
            exit = true;
        }

        exit = false;
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
        let mut pearls = self.group.write().compat().boxed().await.map_err(|e| {
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

    fn read_vdisk_directory(&self) -> BackendResult<(Vec<PearlTimestampHolder<TSpawner>>)>{
        Stuff::check_or_create_directory(&self.directory_path)?;

        let mut pearls = vec![];
        match read_dir(self.directory_path.clone()) {
            Ok(dir)=>{
                for entry in dir{
                    if let Ok(entry) = entry {
                        if let Ok(metadata) = entry.metadata() {
                            if !metadata.is_dir() {
                                trace!("ignore: {:?}", entry);
                                continue;
                            }
                            let file_name = entry
                                .file_name()
                                .into_string()
                                .map_err(|_| warn!("cannot parse file name: {:?}", entry));
                            if file_name.is_err(){
                                continue;
                            }
                            let timestamp:Result<u32, _> = file_name.unwrap().parse()
                                .map_err(|_| warn!("cannot parse file name: {:?} as timestamp", entry));
                            let start_timestamp = timestamp.unwrap();
                            let end_timestamp = start_timestamp + 100; // TODO get value from config
                            let pearl = PearlHolder::new(
                                &self.disk_name,
                                self.vdisk_id.clone(),
                                entry.path(),
                                self.config.clone(),
                                self.spawner.clone(),
                            );
                            let pearl_holder = PearlTimestampHolder::new(pearl, start_timestamp, end_timestamp);
                            pearls.push(pearl_holder);
                        } else {
                            debug!("Couldn't get metadata for {:?}", entry.path());
                            return Err(backend::Error::Failed(format!("Couldn't get metadata for {:?}", entry.path())));
                        }
                    }
                    else {
                        debug!("couldn't read entry: {:?} ", entry);
                        return Err(backend::Error::Failed(format!("couldn't read entry: {:?}", entry)));
                    }
                }
            },
            Err(err)=>{
                debug!("couldn't process path: {:?}, error: {:?} ", self.directory_path, err);
                return Err(backend::Error::Failed(format!("couldn't process path: {:?}, error: {:?} ", self.directory_path, err)));
            },
        }

        Ok(pearls)
    }

    pub async fn add(&self, pearl: PearlTimestampHolder<TSpawner>) -> BackendResult<()>{
        let mut pearls = self.group.write().compat().boxed().await.map_err(|e| {
            error!("cannot take lock: {:?}", e);
            backend::Error::Failed(format!("cannot take lock: {:?}", e))
        })?;

        pearls.push(pearl);
    
        Ok(())
    }

    pub async fn add_range(&self, mut new_pearls: Vec<PearlTimestampHolder<TSpawner>>) -> BackendResult<()>{
        let mut pearls = self.group.write().compat().boxed().await.map_err(|e| {
            error!("cannot take lock: {:?}", e);
            backend::Error::Failed(format!("cannot take lock: {:?}", e))
        })?;

        pearls.append(&mut new_pearls);
    
        Ok(())
    }

    fn get_actual(&self, list: Vec<PearlTimestampHolder<TSpawner>>, key: BobKey, data: BobData) -> BackendResult<PearlTimestampHolder<TSpawner>> {
        let mut i = list.len() - 1;
        while i >= 0 {
            if self.settings.is_actual(list[i].clone(), key, data.clone()) //TODO add pointer to remove clonning
                {return Ok(list[i].clone());}
            i-=1;
        }
        error!("cannot find actual pearl folder. key: {}, meta: {}",key, data.meta);
        return Err(backend::Error::Failed(format!("cannot find actual pearl folder. key: {}, meta: {}",key, data.meta)));
    }

    pub async fn put(&self, key: BobKey, data: BobData) -> PutResult {
        let pearls = self.group.read().compat().boxed().await.map_err(|e| {
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
        let pearls = self.group.read().compat().boxed().await.map_err(|e| {
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
                },
                Err(err) => {
                    has_error = true;
                    debug!("get error: {}, from : {}", err, pearl);
                },
            }
        }
        if results.len() == 0 {
            if has_error {
                debug!("cannot read from some pearls");
                return Err(backend::Error::Failed("cannot read from some pearls".to_string()));
            }
            else{
                return Err(backend::Error::KeyNotFound);
            }
        }
        // self.settings.choose_data(results)
        unimplemented!();
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