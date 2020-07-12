const DOCKER_CONFIGS_DIR: &'static str = "/configs";
const DOCKER_DISKS_DIR: &'static str = "/tmp";
const DOCKER_SSH_DIR: &'static str = "/root/local_ssh";

pub struct DockerFSConstants {}

impl DockerFSConstants {
    pub fn docker_configs_dir() -> String {
        DOCKER_CONFIGS_DIR.to_string()
    }

    pub fn docker_disks_dir() -> String {
        DOCKER_DISKS_DIR.to_string()
    }

    pub fn docker_ssh_dir() -> String {
        DOCKER_SSH_DIR.to_string()
    }
}
