const DOCKER_CONFIGS_DIR: &str = "/configs";
const DOCKER_DISKS_DIR: &str = "/tmp";

pub struct DockerFSConstants {}

impl DockerFSConstants {
    pub fn docker_configs_dir() -> String {
        DOCKER_CONFIGS_DIR.to_string()
    }

    pub fn docker_disks_dir() -> String {
        DOCKER_DISKS_DIR.to_string()
    }
}
