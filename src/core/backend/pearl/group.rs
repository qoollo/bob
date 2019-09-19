use super::{
    holder::PearlHolder,
    data::BackendResult,
    settings::Settings,
    };
use crate::core::{
    backend,
    data::{BobData, BobKey, VDiskId},
    backend::core::*,
    };
use std::sync::Arc;
use futures03::{
    compat::Future01CompatExt,
    task::Spawn,
    FutureExt,
};
use futures_locks::RwLock;

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
}

impl<TSpawner: Spawn + Clone + Send + 'static + Unpin + Sync> PearlGroup<TSpawner> {
    pub fn new(settings: Arc<Settings>) -> Self {
        PearlGroup {
            group: Arc::new(RwLock::new(vec![])),
            settings,
        }
    }
    pub async fn add(&self, pearl: PearlTimestampHolder<TSpawner>) -> BackendResult<()>{
        let mut lock = self.group.write().compat().boxed().await.map_err(|e| {
            error!("cannot take lock: {:?}", e);
            backend::Error::Failed(format!("cannot take lock: {:?}", e))
        })?;

        lock.push(pearl);
    
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
        let folders = self.group.read().compat().boxed().await.map_err(|e| {
            error!("cannot take lock: {:?}", e);
            backend::Error::Failed(format!("cannot take lock: {:?}", e))
        })?;

        let pearl = self.get_actual(folders.to_vec(), key, data.clone())?;

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
        let folders = self.group.read().compat().boxed().await.map_err(|e| {
            error!("cannot take lock: {:?}", e);
            backend::Error::Failed(format!("cannot take lock: {:?}", e))
        })?;

        let mut has_error = false;
        let mut results = vec![];
        for pearl in folders.iter() {
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