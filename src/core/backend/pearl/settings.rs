use super::{
    data::BackendResult,
    group::{PearlGroup, PearlTimestampHolder},
    stuff::*,
};
use crate::core::{
    backend,
    backend::core::*,
    configs::node::NodeConfig,
    data::{BobData, BobKey, VDiskId},
    mapper::VDiskMapper,
};

use futures03::task::Spawn;
use std::{
    fs::{read_dir, DirEntry, Metadata},
    marker::PhantomData,
    path::PathBuf,
    sync::Arc,
};

pub(crate) struct Settings<TSpawner> {
    bob_prefix_path: String,
    alien_folder: PathBuf,
    mapper: Arc<VDiskMapper>,

    phantom: PhantomData<TSpawner>,
}

impl<TSpawner: Spawn + Clone + Send + 'static + Unpin + Sync> Settings<TSpawner> {
    pub(crate) fn new(config: &NodeConfig, mapper: Arc<VDiskMapper>) -> Self {
        let pearl_config = config.pearl.clone().unwrap();

        let alien_folder = format!(
            "{}/{}/",
            mapper
                .get_disk_by_name(&pearl_config.alien_disk())
                .expect("cannot find alien disk in config")
                .path,
            pearl_config.settings().alien_root_dir_name()
        );

        Settings {
            bob_prefix_path: pearl_config.settings().root_dir_name(),
            alien_folder: PathBuf::from(alien_folder),
            mapper,
            phantom: PhantomData,
        }
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
            let file_name = entry
                .file_name()
                .into_string()
                .map_err(|_| warn!("cannot parse file name: {:?}", entry));
            if file_name.is_err() {
                continue;
            }
            let timestamp: Result<u32, _> = file_name
                .unwrap()
                .parse()
                .map_err(|_| warn!("cannot parse file name: {:?} as timestamp", entry));
            let start_timestamp = timestamp.unwrap();
            let end_timestamp = start_timestamp + 100; // TODO get value from config
            let pearl_holder = PearlTimestampHolder::new(
                group.create_pearl_by_path(entry.path()),
                start_timestamp,
                end_timestamp,
            );
            pearls.push(pearl_holder);
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
            let name = self.try_parse_node_name(node);
            if name.is_err() {
                trace!("ignore: {:?}", name);
                continue;
            }
            let (node, name) = name.unwrap();
            let vdisks = self.get_all_subdirectories(node.path())?;

            for vdisk_id in vdisks.into_iter() {
                let id = self.try_parse_vdisk_id(vdisk_id);
                if id.is_err() {
                    trace!("ignore: {:?}", id);
                    continue;
                }
                let (vdisk_id, id) = id.unwrap();
                if self.mapper.is_node_holds_vdisk(&name, id.clone()) {
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
        Ok(result)
    }

    fn get_all_subdirectories(&self, path: PathBuf) -> BackendResult<Vec<DirEntry>> {
        Stuff::check_or_create_directory(&path)?;

        let mut directories = vec![];
        match read_dir(path.clone()) {
            Ok(dir) => {
                for entry in dir {
                    let (entry, metadata) = self.try_read_path(entry)?;
                    if !metadata.is_dir() {
                        trace!("ignore: {:?}", entry);
                        continue;
                    }
                    directories.push(entry);
                }
            }
            Err(err) => {
                debug!("couldn't process path: {:?}, error: {:?} ", path, err);
                return Err(backend::Error::Failed(format!(
                    "couldn't process path: {:?}, error: {:?} ",
                    path, err
                )));
            }
        }
        Ok(directories)
    }

    fn try_parse_node_name(&self, entry: DirEntry) -> BackendResult<(DirEntry, String)> {
        let file_name = entry.file_name().into_string().map_err(|_| {
            warn!("cannot parse file name: {:?}", entry);
            backend::Error::Other
        })?;
        let node = self
            .mapper
            .nodes()
            .iter()
            .find(|node| node.name == file_name);
        node.map(|n| (entry, n.name().clone())).ok_or({
            debug!("cannot find node with name: {:?}", file_name);
            backend::Error::Failed(format!("cannot find node with name: {:?}", file_name))
        })
    }

    fn try_parse_vdisk_id(&self, entry: DirEntry) -> BackendResult<(DirEntry, VDiskId)> {
        let file_name = entry.file_name().into_string().map_err(|_| {
            warn!("cannot parse file name: {:?}", entry);
            backend::Error::Other
        })?;
        let vdisk_id = VDiskId::new(file_name.parse().map_err(|_| {
            warn!("cannot parse file name: {:?} as vdisk id", entry);
            backend::Error::Other
        })?);

        let vdisk = self
            .mapper
            .get_vdisks_ids()
            .into_iter()
            .find(|vdisk| *vdisk == vdisk_id);
        vdisk.map(|id| (entry, id.clone())).ok_or({
            debug!("cannot find vdisk with id: {:?}", vdisk_id);
            backend::Error::Failed(format!("cannot find vdisk with id: {:?}", vdisk_id))
        })
    }

    fn try_read_path(
        &self,
        entry: Result<DirEntry, std::io::Error>,
    ) -> BackendResult<(DirEntry, Metadata)> {
        if let Ok(entry) = entry {
            if let Ok(metadata) = entry.metadata() {
                Ok((entry, metadata))
            } else {
                debug!("Couldn't get metadata for {:?}", entry.path());
                Err(backend::Error::Failed(format!(
                    "Couldn't get metadata for {:?}",
                    entry.path()
                )))
            }
        } else {
            debug!("couldn't read entry: {:?} ", entry);
            Err(backend::Error::Failed(format!(
                "couldn't read entry: {:?}",
                entry
            )))
        }
    }

    fn normal_path(&self, disk_path: &str, vdisk_id: &VDiskId) -> PathBuf {
        let mut vdisk_path = PathBuf::from(format!("{}/{}/", disk_path, self.bob_prefix_path));
        vdisk_path.push(format!("{}/", vdisk_id));
        vdisk_path
    }

    pub(crate) fn is_actual(
        &self,
        pearl: PearlTimestampHolder<TSpawner>,
        _key: BobKey,
        data: BobData,
    ) -> bool {
        pearl.start_timestamp <= data.meta.timestamp && data.meta.timestamp < pearl.end_timestamp
    }

    pub(crate) fn choose_data(&self, records: Vec<BackendGetResult>) -> GetResult {
        if records.is_empty() {
            return Err(backend::Error::KeyNotFound);
        }
        let mut iter = records.into_iter().enumerate();
        let first = iter.next().unwrap();
        let result = iter.try_fold(first, |max, x| {
            let r = if max.1.data.meta.timestamp > x.1.data.meta.timestamp {
                max
            } else {
                x
            };
            Some(r)
        });

        Ok(result.unwrap().1)
    }
}
