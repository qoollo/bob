// use super::core::BackendOperation;
use crate::core::{configs::node::{NodeConfig, PearlConfig}, data::{BobData, BobKey, VDiskId}, mapper::VDiskMapper, backend, backend::core::*,};
use super::{
    data::BackendResult,
    group::{PearlTimestampHolder, PearlGroup},
    holder::PearlHolder,
    stuff::*};

use std::{path::PathBuf, sync::Arc, fs::read_dir};
use futures03::{
    task::Spawn,
};

pub(crate) struct Settings {
    bob_prefix_path: String,
    alien_folder: String,
    mapper: Arc<VDiskMapper>,
}

impl Settings {
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
            alien_folder,
            mapper,
        }
    }

    pub(crate) async fn read_pearl_from_disk<TSpawner: Spawn + Clone + Send + 'static + Unpin + Sync>
     (&self, settings: Arc<Settings>, config: PearlConfig, spawner: TSpawner) -> Vec<PearlGroup<TSpawner>> {
        let mut result = vec![];
        for disk in self.mapper.local_disks().iter() {
            let mut vdisks: Vec<_> = self.mapper
                .get_vdisks_by_disk(&disk.name)
                .iter()
                .map(|vdisk_id| {
                    let path = self.normal_directory(&disk.path, &vdisk_id);
                    PearlGroup::<TSpawner>::new(settings.clone(), vdisk_id.clone(), disk.name.clone(), path.clone(), config.clone(), spawner.clone())
                })
                .collect();
            result.append(&mut vdisks);
        }
        result
    }
    pub(crate) fn normal_directory(&self, disk_path: &str, vdisk_id: &VDiskId) -> PathBuf {
        let mut vdisk_path = PathBuf::from(format!("{}/{}/", disk_path, self.bob_prefix_path));
        vdisk_path.push(format!("{}/", vdisk_id));
        vdisk_path
    }

    pub(crate) fn alien_directory(&self) -> PathBuf {
        PathBuf::from(self.alien_folder.clone())
    }

    pub(crate) fn is_actual<TSpawner: Spawn + Clone + Send + 'static + Unpin + Sync>(&self, pearl: PearlTimestampHolder<TSpawner>, _key: BobKey, data: BobData) -> bool {
        pearl.start_timestamp <= data.meta.timestamp && data.meta.timestamp < pearl.end_timestamp
    }

    pub(crate) fn choose_data(&self, records: Vec<BackendGetResult>) -> GetResult {
        if records.len() == 0 {
            return Err(backend::Error::KeyNotFound);
        }
        let mut iter = records.into_iter().enumerate();
        let first = iter.next().unwrap();
        let result = iter.try_fold(first, |max, x| {
            let r = if max.1.data.meta.timestamp > x.1.data.meta.timestamp {
                max
            } else { x };
            Some(r)
        }); 

        Ok(result.unwrap().1)
    }
}