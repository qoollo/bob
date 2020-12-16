use super::prelude::*;

#[derive(Debug, Clone)]
pub struct WorkDir {
    mount_point: MountPoint,
    path: PathBuf,
}

#[derive(Debug, Clone, PartialEq)]
enum MountPoint {
    Dir,
    Device,
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
#[tokio::test]
async fn test_work_dir_from_path_buf() {
    let path1 = PathBuf::from("/dev/sdd");
    let path2 = PathBuf::from("/dev/sdd/jia/ojh/vuj");
    let path3 = PathBuf::from("/dev/s");
    let path4 = PathBuf::from("dev/sdd");
    let path5 = PathBuf::from("/de/sda");
    let path6 = PathBuf::from("/home/rust");
    assert!(WorkDir::from(path1).mount_point == MountPoint::Device);
    assert!(WorkDir::from(path2).mount_point == MountPoint::Device);
    assert!(WorkDir::from(path3).mount_point == MountPoint::Dir);
    assert!(WorkDir::from(path4).mount_point == MountPoint::Dir);
    assert!(WorkDir::from(path5).mount_point == MountPoint::Dir);
    assert!(WorkDir::from(path6).mount_point == MountPoint::Dir);
}
