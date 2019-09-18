// use super::core::BackendOperation;
use crate::core::{
    data::VDiskId,
    configs::node::NodeConfig,
    mapper::VDiskMapper,
};


use std::{path::PathBuf, sync::Arc};

pub(crate) struct BackendPolicy {
    bob_prefix_path: String,
    alien_folder: String,
}

impl BackendPolicy {
    pub(crate) fn new(config: &NodeConfig, mapper: Arc<VDiskMapper>) -> Self {
        let pearl_config = config.pearl.clone().unwrap();

        let alien_folder = format!(
            "{}/alien/",
            mapper
                .get_disk_by_name(&pearl_config.alien_disk())
                .unwrap()
                .path
        );

        BackendPolicy {
            bob_prefix_path: "bob".to_string(),
            alien_folder: alien_folder,
        }
    }

    pub(crate) fn normal_directory(&self, disk_path: &str, vdisk_id: &VDiskId) -> PathBuf {
        let mut vdisk_path = PathBuf::from(format!("{}/{}/", disk_path, self.bob_prefix_path));
        vdisk_path.push(format!("{}/", vdisk_id));
        vdisk_path
    }

    pub(crate) fn alien_directory(&self) -> PathBuf {
        PathBuf::from(self.alien_folder.clone())
    }
}
