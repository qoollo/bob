use super::prelude::*;

/// Contains timestamp and fs logic
pub(crate) struct Settings<TSpawner> {
    bob_prefix_path: String,
    alien_folder: PathBuf,
    timestamp_period: Duration,
    pub config: PearlConfig,
    spawner: TSpawner,
    mapper: Arc<VDiskMapper>,
}

impl<TSpawner> Settings<TSpawner>
where
    TSpawner: Spawn + Clone + Send + 'static + Unpin + Sync,
{
    pub(crate) fn new(config: &NodeConfig, mapper: Arc<VDiskMapper>, spawner: TSpawner) -> Self {
        let pearl_config = config.pearl.clone().expect("get pearl config");

        let alien_folder = format!(
            "{}/{}/",
            mapper
                .get_disk_by_name(&pearl_config.alien_disk())
                .expect("cannot find alien disk in config")
                .path,
            pearl_config.settings().alien_root_dir_name()
        )
        .into();

        Settings {
            bob_prefix_path: pearl_config.settings().root_dir_name(),
            alien_folder,
            timestamp_period: pearl_config.settings().timestamp_period(),
            mapper,
            spawner,
            config: pearl_config,
        }
    }

    pub(crate) fn create_current_pearl(
        &self,
        group: &PearlGroup<TSpawner>,
    ) -> BackendResult<PearlTimestampHolder<TSpawner>> {
        let start_timestamp = self.get_current_timestamp_start()?;
        let end_timestamp = start_timestamp + self.get_timestamp_period()?;
        let mut path = group.directory_path.clone();
        path.push(format!("{}/", start_timestamp));

        Ok(PearlTimestampHolder::new(
            group.create_pearl_by_path(path),
            start_timestamp,
            end_timestamp,
        ))
    }

    pub(crate) fn create_pearl(
        &self,
        group: &PearlGroup<TSpawner>,
        data: BobData,
    ) -> BackendResult<PearlTimestampHolder<TSpawner>> {
        let start_timestamp =
            Stuff::get_start_timestamp_by_timestamp(self.timestamp_period, data.meta.timestamp)?;

        let end_timestamp = start_timestamp + self.get_timestamp_period()?;
        let mut path = group.directory_path.clone();
        path.push(format!("{}/", start_timestamp));

        Ok(PearlTimestampHolder::new(
            group.create_pearl_by_path(path),
            start_timestamp,
            end_timestamp,
        ))
    }

    pub(crate) fn read_group_from_disk(
        &self,
        settings: Arc<Settings<TSpawner>>,
        config: &NodeConfig,
        spawner: TSpawner,
    ) -> Vec<PearlGroup<TSpawner>> {
        let mut result = vec![];
        for disk in self.mapper.local_disks().iter() {
            let mut vdisks: Vec<_> = self
                .mapper
                .get_vdisks_by_disk(&disk.name)
                .iter()
                .map(|vdisk_id| {
                    let path = self.normal_path(&disk.path, &vdisk_id);

                    PearlGroup::<TSpawner>::new(
                        settings.clone(),
                        vdisk_id.clone(),
                        config.name(),
                        disk.name.clone(),
                        path.clone(),
                        config.pearl(),
                        spawner.clone(),
                    )
                })
                .collect();
            result.append(&mut vdisks);
        }
        result
    }

    pub(crate) fn read_vdisk_directory(
        &self,
        group: &PearlGroup<TSpawner>,
    ) -> BackendResult<(Vec<PearlTimestampHolder<TSpawner>>)> {
        Stuff::check_or_create_directory(&group.directory_path)?;

        let mut pearls = vec![];
        let pearl_directories = self.get_all_subdirectories(group.directory_path.clone())?;
        for entry in pearl_directories.into_iter() {
            if let Ok(file_name) = entry
                .file_name()
                .into_string()
                .map_err(|_| warn!("cannot parse file name: {:?}", entry))
            {
                let start_timestamp: i64 = file_name
                    .parse()
                    .map_err(|_| warn!("cannot parse file name: {:?} as timestamp", entry))
                    .expect("parse file name");
                let end_timestamp = start_timestamp + self.get_timestamp_period()?;
                let pearl_holder = PearlTimestampHolder::new(
                    group.create_pearl_by_path(entry.path()),
                    start_timestamp,
                    end_timestamp,
                );
                trace!("read pearl: {}", pearl_holder);
                pearls.push(pearl_holder);
            }
        }
        Ok(pearls)
    }

    pub(crate) fn read_alien_directory(
        &self,
        settings: Arc<Settings<TSpawner>>,
        config: &NodeConfig,
        spawner: TSpawner,
    ) -> BackendResult<Vec<PearlGroup<TSpawner>>> {
        let mut result = vec![];

        let node_names = self.get_all_subdirectories(self.alien_folder.clone())?;
        for node in node_names.into_iter() {
            if let Ok((node, name)) = self.try_parse_node_name(node) {
                let vdisks = self.get_all_subdirectories(node.path())?;

                for vdisk_id in vdisks.into_iter() {
                    if let Ok((vdisk_id, id)) = self.try_parse_vdisk_id(vdisk_id) {
                        if self.mapper.does_node_holds_vdisk(&name, id.clone()) {
                            let pearl = config.pearl();
                            let group = PearlGroup::<TSpawner>::new(
                                settings.clone(),
                                id.clone(),
                                name.clone(),
                                pearl.alien_disk(),
                                vdisk_id.path().clone(),
                                pearl,
                                spawner.clone(),
                            );
                            result.push(group);
                        } else {
                            warn!(
                                "potentionally invalid state. Node: {} doesnt hold vdisk: {}",
                                name, id
                            );
                        }
                    }
                }
            }
        }
        Ok(result)
    }

    pub(crate) fn create_group(
        &self,
        operation: BackendOperation,
        settings: Arc<Settings<TSpawner>>,
    ) -> BackendResult<PearlGroup<TSpawner>> {
        let id = operation.vdisk_id.clone();
        let path = self.alien_path(&id, &operation.remote_node_name());

        Stuff::check_or_create_directory(&path)?;

        let group = PearlGroup::<TSpawner>::new(
            settings.clone(),
            id.clone(),
            operation.remote_node_name(),
            self.config.alien_disk(),
            path,
            self.config.clone(),
            self.spawner.clone(),
        );
        Ok(group)
    }

    fn get_all_subdirectories(&self, path: PathBuf) -> BackendResult<Vec<DirEntry>> {
        Stuff::check_or_create_directory(&path)?;

        match read_dir(path.clone()) {
            Ok(dir) => {
                let mut directories = vec![];
                for entry in dir {
                    let (entry, metadata) = self.try_read_path(entry)?;
                    if metadata.is_dir() {
                        directories.push(entry);
                    } else {
                        trace!("ignore: {:?}", entry);
                        continue;
                    }
                }
                Ok(directories)
            }
            Err(err) => {
                debug!("couldn't process path: {:?}, error: {:?} ", path, err);
                Err(Error::Failed(format!(
                    "couldn't process path: {:?}, error: {:?} ",
                    path, err
                )))
            }
        }
    }

    fn try_parse_node_name(&self, entry: DirEntry) -> BackendResult<(DirEntry, String)> {
        let file_name = entry.file_name().into_string().map_err(|_| {
            warn!("cannot parse file name: {:?}", entry);
            Error::Failed(format!("cannot parse file name: {:?}", entry))
        })?;
        let node = self
            .mapper
            .nodes()
            .iter()
            .find(|node| node.name == file_name);
        node.map(|n| (entry, n.name().clone())).ok_or({
            debug!("cannot find node with name: {:?}", file_name);
            Error::Failed(format!("cannot find node with name: {:?}", file_name))
        })
    }

    fn try_parse_vdisk_id(&self, entry: DirEntry) -> BackendResult<(DirEntry, VDiskId)> {
        let file_name = entry.file_name().into_string().map_err(|_| {
            warn!("cannot parse file name: {:?}", entry);
            Error::Failed(format!("cannot parse file name: {:?}", entry))
        })?;
        let vdisk_id = VDiskId::new(file_name.parse().map_err(|_| {
            warn!("cannot parse file name: {:?} as vdisk id", entry);
            Error::Failed(format!("cannot parse file name: {:?}", entry))
        })?);

        let vdisk = self
            .mapper
            .get_vdisks_ids()
            .into_iter()
            .find(|vdisk| *vdisk == vdisk_id);
        vdisk.map(|id| (entry, id.clone())).ok_or({
            debug!("cannot find vdisk with id: {:?}", vdisk_id);
            Error::Failed(format!("cannot find vdisk with id: {:?}", vdisk_id))
        })
    }

    fn try_read_path(&self, entry: IOResult<DirEntry>) -> BackendResult<(DirEntry, Metadata)> {
        if let Ok(entry) = entry {
            if let Ok(metadata) = entry.metadata() {
                Ok((entry, metadata))
            } else {
                debug!("Couldn't get metadata for {:?}", entry.path());
                Err(Error::Failed(format!(
                    "Couldn't get metadata for {:?}",
                    entry.path()
                )))
            }
        } else {
            debug!("couldn't read entry: {:?} ", entry);
            Err(Error::Failed(format!("couldn't read entry: {:?}", entry)))
        }
    }

    fn normal_path(&self, disk_path: &str, vdisk_id: &VDiskId) -> PathBuf {
        let mut vdisk_path = PathBuf::from(format!("{}/{}/", disk_path, self.bob_prefix_path));
        vdisk_path.push(format!("{}/", vdisk_id));
        vdisk_path
    }

    fn alien_path(&self, vdisk_id: &VDiskId, node_name: &str) -> PathBuf {
        let mut vdisk_path = self.alien_folder.clone();
        vdisk_path.push(format!("{}/{}/", node_name, vdisk_id));
        vdisk_path
    }

    #[inline]
    fn get_timestamp_period(&self) -> BackendResult<i64> {
        Stuff::get_period_timestamp(self.timestamp_period)
    }

    #[inline]
    fn get_current_timestamp_start(&self) -> BackendResult<i64> {
        Stuff::get_start_timestamp_by_std_time(self.timestamp_period, SystemTime::now())
    }

    #[inline]
    pub(crate) fn is_actual(&self, pearl: &PearlTimestampHolder<TSpawner>, data: &BobData) -> bool {
        trace!(
            "start: {}, end: {}, check: {}",
            pearl.start_timestamp,
            pearl.end_timestamp,
            data.meta.timestamp
        );
        pearl.start_timestamp <= data.meta.timestamp && data.meta.timestamp < pearl.end_timestamp
    }

    #[inline]
    pub(crate) fn choose_data(&self, records: Vec<BackendGetResult>) -> GetResult {
        records
            .into_iter()
            .max_by(|x, y| x.data.meta.timestamp.cmp(&y.data.meta.timestamp))
            .ok_or(Error::KeyNotFound)
    }

    pub(crate) fn is_actual_pearl(
        &self,
        pearl: &PearlTimestampHolder<TSpawner>,
    ) -> BackendResult<bool> {
        Ok(pearl.start_timestamp == self.get_current_timestamp_start()?)
    }
}
