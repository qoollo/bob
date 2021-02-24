use super::prelude::*;

#[derive(Debug)]
pub(crate) struct Settings {
    bob_prefix_path: String,
    alien_folder: PathBuf,
    timestamp_period: Duration,
    config: PearlConfig,
    mapper: Arc<Virtual>,
}

impl Settings {
    pub(crate) fn new(config: &NodeConfig, mapper: Arc<Virtual>) -> Self {
        let config = config.pearl().clone();
        let alien_folder = if let Some(alien_disk) = config.alien_disk() {
            let disk_path = mapper
                .get_disk(alien_disk)
                .expect("cannot find alien disk in config")
                .path();
            format!("{}/{}/", disk_path, config.settings().alien_root_dir_name()).into()
        } else {
            config.settings().alien_root_dir_name().into()
        };

        Self {
            bob_prefix_path: config.settings().root_dir_name().to_owned(),
            alien_folder,
            timestamp_period: config.settings().timestamp_period(),
            mapper,
            config,
        }
    }

    pub(crate) fn config(&self) -> &PearlConfig {
        &self.config
    }

    pub(crate) fn read_group_from_disk(
        self: Arc<Self>,
        config: &NodeConfig,
        run_sem: Arc<Semaphore>,
    ) -> Vec<Arc<DiskController>> {
        self.mapper
            .local_disks()
            .iter()
            .map(|disk| {
                let vdisks = self.mapper.get_vdisks_by_disk(disk.name());
                DiskController::new(
                    disk.to_owned(),
                    vdisks,
                    config,
                    run_sem.clone(),
                    self.clone(),
                    false,
                )
            })
            .collect()
    }

    pub(crate) fn read_alien_directory(
        self: Arc<Self>,
        config: &NodeConfig,
        run_sem: Arc<Semaphore>,
    ) -> BackendResult<Arc<DiskController>> {
        let disk_name = config
            .pearl()
            .alien_disk()
            .map_or_else(String::new, str::to_owned);
        let alien_disk = DiskPath::new(
            disk_name,
            self.alien_folder
                .clone()
                .into_os_string()
                .into_string()
                .expect("Path is not utf8 encoded"),
        );
        let dc = DiskController::new(alien_disk, Vec::new(), config, run_sem, self.clone(), true);
        Ok(dc)
    }

    pub(crate) fn collect_alien_groups(
        self: Arc<Self>,
        disk_name: String,
    ) -> BackendResult<Vec<Group>> {
        let mut result = vec![];
        let node_names = Self::get_all_subdirectories(&self.alien_folder)?;
        for node in node_names {
            if let Ok((node, node_name)) = self.try_parse_node_name(node) {
                let vdisks = Self::get_all_subdirectories(&node.path())?;

                for vdisk_id in vdisks {
                    if let Ok((entry, vdisk_id)) = self.try_parse_vdisk_id(vdisk_id) {
                        if self.mapper.is_vdisk_on_node(&node_name, vdisk_id) {
                            let group = Group::new(
                                self.clone(),
                                vdisk_id,
                                node_name.clone(),
                                disk_name.clone(),
                                entry.path(),
                                node_name.clone(),
                                Arc::new(Semaphore::new(1)),
                            );
                            result.push(group);
                        } else {
                            warn!(
                                "potentionally invalid state. Node: {} doesn't hold vdisk: {}",
                                node_name, vdisk_id
                            );
                        }
                    }
                }
            }
        }
        Ok(result)
    }

    pub(crate) fn create_group(
        self: Arc<Self>,
        operation: &Operation,
        node_name: &str,
        dump_sem: Arc<Semaphore>,
    ) -> BackendResult<Group> {
        let remote_node_name = operation.remote_node_name().unwrap();
        let path = self.alien_path(operation.vdisk_id(), remote_node_name);

        Stuff::check_or_create_directory(&path)?;

        let disk_name = self
            .config
            .alien_disk()
            .map_or_else(String::new, str::to_owned);
        let group = Group::new(
            self.clone(),
            operation.vdisk_id(),
            remote_node_name.to_owned(),
            disk_name,
            path,
            node_name.to_owned(),
            dump_sem,
        );
        Ok(group)
    }

    pub fn get_all_subdirectories(path: &Path) -> BackendResult<Vec<DirEntry>> {
        Stuff::check_or_create_directory(path)?;

        match read_dir(path) {
            Ok(dir) => {
                let mut directories = vec![];
                for entry in dir {
                    let (entry, metadata) = Self::try_read_path(entry)?;
                    if metadata.is_dir() {
                        directories.push(entry);
                    }
                }
                Ok(directories)
            }
            Err(err) => {
                let msg = format!("couldn't process path: {:?}, error: {:?} ", path, err);
                error!("{}", msg);
                Err(Error::failed(msg))
            }
        }
    }

    fn try_parse_node_name(&self, entry: DirEntry) -> BackendResult<(DirEntry, String)> {
        let file_name = entry.file_name().into_string().map_err(|e| {
            error!("cannot parse file name: {:?}, {:?}", entry, e);
            Error::failed(format!("cannot parse file name: {:?}", entry))
        })?;
        if self
            .mapper
            .nodes()
            .values()
            .any(|node| node.name() == file_name)
        {
            Ok((entry, file_name))
        } else {
            let msg = format!("cannot find node with name: {:?}", file_name);
            error!("{}", msg);
            Err(Error::failed(msg))
        }
    }

    fn try_parse_vdisk_id(&self, entry: DirEntry) -> BackendResult<(DirEntry, VDiskID)> {
        let file_name = entry.file_name().into_string().map_err(|_| {
            let msg = format!("cannot parse file name: {:?}", entry);
            error!("{}", msg);
            Error::failed(msg)
        })?;
        let vdisk_id: VDiskID = file_name.parse().map_err(|_| {
            let msg = format!("cannot parse file name: {:?}", entry);
            error!("{}", msg);
            Error::failed(msg)
        })?;

        let vdisk = self
            .mapper
            .get_vdisks_ids()
            .into_iter()
            .find(|vdisk| *vdisk == vdisk_id);
        vdisk.map(|id| (entry, id)).ok_or({
            let msg = format!("cannot find vdisk with id: {:?}", vdisk_id);
            error!("{}", msg);
            Error::failed(msg)
        })
    }

    fn try_read_path(entry: IOResult<DirEntry>) -> BackendResult<(DirEntry, Metadata)> {
        if let Ok(entry) = entry {
            if let Ok(metadata) = entry.metadata() {
                Ok((entry, metadata))
            } else {
                let msg = format!("Couldn't get metadata for {:?}", entry.path());
                error!("{}", msg);
                Err(Error::failed(msg))
            }
        } else {
            let msg = format!("couldn't read entry: {:?} ", entry);
            error!("{}", msg);
            Err(Error::failed(msg))
        }
    }

    pub(crate) fn normal_path(&self, disk_path: &str, vdisk_id: VDiskID) -> PathBuf {
        let mut vdisk_path = PathBuf::from(format!("{}/{}/", disk_path, self.bob_prefix_path));
        vdisk_path.push(format!("{}/", vdisk_id));
        vdisk_path
    }

    fn alien_path(&self, vdisk_id: VDiskID, node_name: &str) -> PathBuf {
        let mut vdisk_path = self.alien_folder.clone();
        vdisk_path.push(format!("{}/{}/", node_name, vdisk_id));
        vdisk_path
    }

    #[inline]
    pub fn timestamp_period(&self) -> Duration {
        self.timestamp_period
    }

    #[inline]
    pub fn timestamp_period_as_secs(&self) -> u64 {
        self.timestamp_period.as_secs()
    }

    #[inline]
    pub fn get_actual_timestamp_start(&self) -> u64 {
        Stuff::get_start_timestamp_by_std_time(self.timestamp_period, SystemTime::now())
    }

    #[inline]
    pub(crate) fn choose_most_recent_data(records: Vec<BobData>) -> Option<BobData> {
        records
            .into_iter()
            .max_by(|x, y| x.meta().timestamp().cmp(&y.meta().timestamp()))
    }
}
