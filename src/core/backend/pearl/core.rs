use super::prelude::*;

pub(crate) type BackendResult<T> = std::result::Result<T, Error>;
pub(crate) type PearlStorage = Storage<Key>;

#[derive(Clone, Debug)]
pub(crate) struct Pearl {
    settings: Arc<Settings>,
    vdisks_groups: Arc<[Group]>,
    alien_vdisks_groups: Arc<RwLock<Vec<Group>>>,
    node_name: String,
    init_par_degree: usize,
}

impl Pearl {
    pub(crate) fn new(mapper: Arc<Virtual>, config: &NodeConfig) -> Self {
        debug!("initializing pearl backend");
        let settings = Arc::new(Settings::new(config, mapper));

        let data = settings.clone().read_group_from_disk(config);
        let vdisks_groups: Arc<[Group]> = Arc::from(data.as_slice());
        trace!("count vdisk groups: {}", vdisks_groups.len());

        let alien = settings
            .clone()
            .read_alien_directory()
            .expect("vec of pearl groups");
        trace!("count alien vdisk groups: {}", alien.len());
        let alien_vdisks_groups = Arc::new(RwLock::new(alien)); //TODO

        Self {
            settings,
            vdisks_groups,
            alien_vdisks_groups,
            node_name: config.name().to_string(),
            init_par_degree: config.init_par_degree(),
        }
    }

    async fn find_alien_pearl(&self, operation: &Operation) -> BackendResult<Group> {
        Self::find_pearl(self.alien_vdisks_groups.read().await.iter(), operation).ok_or_else(|| {
            Error::failed(format!("cannot find actual alien folder. {:?}", operation))
        })
    }

    fn find_pearl<'pearl, 'op>(
        mut pearls: impl Iterator<Item = &'pearl Group>,
        operation: &'op Operation,
    ) -> Option<Group> {
        pearls
            .find(|group| group.can_process_operation(&operation))
            .cloned()
    }

    async fn get_or_create_alien_pearl(&self, operation: &Operation) -> BackendResult<Group> {
        trace!("try get alien pearl, operation {:?}", operation);
        let pearl = Self::find_pearl(self.alien_vdisks_groups.read().await.iter(), operation);
        if let Some(g) = pearl {
            return Ok(g);
        }

        trace!("create alien pearl, operation {:?}", operation);
        let mut write_lock = self.alien_vdisks_groups.write().await;
        let pearl = Self::find_pearl(write_lock.iter(), operation);
        if let Some(g) = pearl {
            return Ok(g);
        }

        self.settings
            .clone()
            .create_group(operation, &self.node_name)
            .map(|g| {
                write_lock.push(g.clone());
                g
            })
    }
}

#[async_trait]
impl BackendStorage for Pearl {
    async fn run_backend(&self) -> Result<()> {
        use futures::stream::futures_unordered::FuturesUnordered;
        use std::collections::hash_map::Entry;

        debug!("run pearl backend");
        let start = std::time::Instant::now();
        let mut inits_by_disk: HashMap<&str, Vec<_>> = HashMap::new();
        let pearl_groups = self.alien_vdisks_groups.read().await;
        for group in self.vdisks_groups.iter().chain(pearl_groups.iter()) {
            let group_c = group.clone();
            let fut = async move { group_c.run().await };
            match inits_by_disk.entry(group.disk_name()) {
                Entry::Occupied(mut e) => {
                    e.get_mut().push(fut);
                }
                Entry::Vacant(e) => {
                    e.insert(vec![fut]);
                }
            }
        }

        // vec![vec![]; n] macro requires Clone trait, and future does not implement it
        let mut par_buckets: Vec<Vec<_>> = (0..self.init_par_degree).map(|_| vec![]).collect();
        for (ind, (_, futs)) in inits_by_disk.into_iter().enumerate() {
            let buck = ind % self.init_par_degree;
            par_buckets[buck].extend(futs.into_iter());
        }
        let futs = FuturesUnordered::new();
        for futures in par_buckets {
            futs.push(async move {
                for f in futures {
                    f.await?;
                }
                Ok::<(), anyhow::Error>(())
            });
        }
        futs.fold(Ok(()), |s, n| async move { s.and(n) }).await?;

        let dur = std::time::Instant::now() - start;
        debug!("pearl backend init took {:?}", dur);
        Ok(())
    }

    async fn put(&self, op: Operation, key: BobKey, data: BobData) -> Result<(), Error> {
        debug!("PUT[{}] to pearl backend. operation: {:?}", key, op);
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

        let vdisk_group = self.get_or_create_alien_pearl(&op).await;
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

    async fn shutdown(&self) {
        use futures::stream::FuturesUnordered;
        info!("begin shutdown");
        let futures = FuturesUnordered::new();
        let aliens = self.alien_vdisks_groups.read().await;
        for vdisk in self.vdisks_groups.iter().chain(aliens.iter()) {
            let holders = vdisk.holders();
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
        info!("shutting down done");
    }

    async fn blobs_count(&self) -> (usize, usize) {
        let mut cnt = 0;
        for group in self.vdisks_groups.iter() {
            let holders_guard = group.holders();
            let holders = holders_guard.read().await;
            for holder in holders.iter() {
                cnt += holder.blobs_count().await;
            }
        }
        let mut alien_cnt = 0;
        let alien_groups = self.alien_vdisks_groups.read().await;
        for group in alien_groups.iter() {
            let holders_guard = group.holders();
            let holders = holders_guard.read().await;
            for holder in holders.iter() {
                alien_cnt += holder.blobs_count().await;
            }
        }
        (cnt, alien_cnt)
    }

    fn vdisks_groups(&self) -> Option<&[Group]> {
        Some(&self.vdisks_groups)
    }
}
