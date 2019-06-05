use crate::core::backend::backend::*;
use crate::core::data::{BobData, BobKey, VDiskId, VDiskMapper};
use crate::core::configs::node::{NodeConfig, PearlConfig};
//use futures::future::{err, ok};
use pearl::{Builder, Storage};

use futures03::executor::{ThreadPool, ThreadPoolBuilder};

use std::path::{Path, PathBuf};
use std::fs::create_dir_all;

pub struct PearlBackend {
    config: PearlConfig,
    pool: ThreadPool,
    vdisks: Vec<PearlVDisk>,
}

//  /<disk>/bob/<vdisk_id>/<data>/<pearl>

impl PearlBackend {
    pub fn new (config: &NodeConfig) -> Self {
        let pearl_config = config.pearl.clone().unwrap();
        let pool = ThreadPoolBuilder::new()
            .pool_size(pearl_config.pool_count_threads() as usize)
            .create()
            .unwrap();

        PearlBackend {
            config: pearl_config,
            pool,
            vdisks: vec![],
        }
    }

    pub fn init(&mut self, mapper: &VDiskMapper) -> Result<(), String>{
        self.vdisks = Vec::new();
        for disk in mapper.local_disks().iter() {
            let base_path = PathBuf::from(format!("{}/bob/", disk.path));
            Self::check_or_create_directory(&base_path).unwrap();

            let mut vdisks: Vec<PearlVDisk> = mapper
                .get_vdisks_by_disk(&disk.name)
                .iter()
                .map(|vdisk_id| {
                    let mut vdisk_path = base_path.clone();
                    vdisk_path.push(format!("{}/", vdisk_id.clone()));
                    Self::check_or_create_directory(&vdisk_path).unwrap();

                    let mut storage = Self::init_pearl_by_path(vdisk_path, &self.config);
                    self.run_storage(&mut storage);
                    
                    PearlVDisk::new(&disk.path, vdisk_id.clone(), storage)
                })
                .collect();
            self.vdisks.append(&mut vdisks);
        }
        Ok(())
    }
    
    fn check_or_create_directory(path: &Path) -> Result<(), String> {
        if !path.exists() {
            let dir = path.to_str().unwrap();
            return create_dir_all(path)
                .map(|_r| {
                    info!("create directory: {}", dir);
                    ()
                } )
                .map_err(|e| format!("cannot create directory: {}, error: {}", dir, e.to_string()));
        }
        Ok(())
    }

    fn init_pearl_by_path(path: PathBuf, config: &PearlConfig) -> Storage<BobKey> {
        let mut builder = Builder::new()
            .work_dir(path);
        
        builder = match &config.blob_file_name_prefix {
            Some(blob_file_name_prefix) => builder.blob_file_name_prefix(blob_file_name_prefix),
            _ => builder.blob_file_name_prefix("bob"),
        };
        builder = match config.max_data_in_blob {
            Some(max_data_in_blob) => builder.max_data_in_blob(max_data_in_blob),
            _ => builder,
        };
        builder = match config.max_blob_size {
            Some(max_blob_size) => builder.max_blob_size(max_blob_size),
            _ => panic!("'max_blob_size' is not set in pearl config"),
        };

        builder.build().unwrap()
    }

    fn run_storage(&mut self, storage: &mut Storage<BobKey>) {
        self.pool
            .run(storage.init(self.pool.clone()))
            .unwrap();
    }
}

struct PearlVDisk {
    pub disk: String,
    pub vdisk: VDiskId,
    pub storage: Storage<BobKey>,
}

impl PearlVDisk {
    pub fn new (disk:&str, vdisk: VDiskId, storage: Storage<BobKey>)->Self {
        PearlVDisk{
            disk: disk.to_string(),
            vdisk,
            storage,
        }
    }
}

impl BackendStorage for PearlBackend {
    fn put(&self, _disk: String, _vdisk: VDiskId, _key: BobKey, _data: BobData) -> BackendPutFuture {
        unimplemented!();
    }

    fn put_alien(&self, _vdisk: VDiskId, _key: BobKey, _data: BobData) -> BackendPutFuture {
        unimplemented!();
    }

    fn get(&self, _disk: String, _vdisk: VDiskId, _key: BobKey) -> BackendGetFuture {
        unimplemented!();
    }

    fn get_alien(&self, _vdisk: VDiskId, _key: BobKey) -> BackendGetFuture {
        unimplemented!();
    }
}