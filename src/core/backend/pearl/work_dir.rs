use super::prelude::*;

#[derive(Debug, Clone)]
pub struct WorkDir {
    mount_point: MountPoint,
    path: PathBuf,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MountPoint {
    Device,
    Dir,
}

impl WorkDir {
    pub fn as_path(&self) -> &Path {
        self.path.as_path()
    }

    pub fn push<P: AsRef<Path>>(&mut self, path: P) {
        self.path.push(path);
    }

    pub fn file_name(&self) -> Option<&str> {
        self.path.file_name().and_then(std::ffi::OsStr::to_str)
    }

    pub fn device(&self) -> Option<PathBuf> {
        match self.mount_point {
            MountPoint::Device => Some(self.path.iter().take(3).collect()),
            MountPoint::Dir => None,
        }
    }
}

impl<T> From<T> for WorkDir
where
    T: Into<PathBuf>,
{
    fn from(input: T) -> Self {
        let path: PathBuf = input.into();
        let mount_point = if path.to_str().unwrap().starts_with("/dev/sd") {
            MountPoint::Device
        } else {
            MountPoint::Dir
        };
        Self { path, mount_point }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{MountPoint, WorkDir};

    const PATH_WITH_DEVICE: &str = "/dev/sdd";
    const PATH_WITH_DEVICE_WITH_TAIL: &str = "/dev/sdd/jia/ojh/vuj";
    const PATH_WITH_INVALID_DEVICE: &str = "/dev/s";
    const PATH_WITHOUT_LEADING_SLASH: &str = "dev/sdd";
    const PATH_NOT_IN_DEV: &str = "/de/sda";
    const PATH_WITHOUT_DEVICE: &str = "/home/rust";

    #[test]
    fn test_work_dir_device() {
        assert_eq!(
            WorkDir::from(PATH_WITH_DEVICE).device(),
            Some(PathBuf::from("/dev/sdd"))
        );
        assert_eq!(
            WorkDir::from(PATH_WITH_DEVICE_WITH_TAIL).device(),
            Some(PathBuf::from("/dev/sdd"))
        );
        assert!(WorkDir::from(PATH_WITH_INVALID_DEVICE).device().is_none());
        assert!(WorkDir::from(PATH_WITHOUT_LEADING_SLASH).device().is_none());
        assert!(WorkDir::from(PATH_NOT_IN_DEV).device().is_none());
        assert!(WorkDir::from(PATH_WITHOUT_DEVICE).device().is_none());
    }

    #[test]
    fn test_work_dir_from_path_buf() {
        assert_eq!(
            WorkDir::from(PATH_WITH_DEVICE).mount_point,
            MountPoint::Device
        );
        assert_eq!(
            WorkDir::from(PATH_WITH_DEVICE_WITH_TAIL).mount_point,
            MountPoint::Device
        );
        assert_eq!(
            WorkDir::from(PATH_WITH_INVALID_DEVICE).mount_point,
            MountPoint::Dir
        );
        assert_eq!(
            WorkDir::from(PATH_WITHOUT_LEADING_SLASH).mount_point,
            MountPoint::Dir
        );
        assert_eq!(WorkDir::from(PATH_NOT_IN_DEV).mount_point, MountPoint::Dir);
        assert_eq!(
            WorkDir::from(PATH_WITHOUT_DEVICE).mount_point,
            MountPoint::Dir
        );
    }
}
