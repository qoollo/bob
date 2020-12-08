use super::prelude::*;

#[derive(Debug, Clone)]
pub struct WorkDir {
    mount_point: MountPoint,
    path: PathBuf,
}

#[derive(Debug, Clone)]
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

impl From<PathBuf> for WorkDir {
    fn from(path: PathBuf) -> Self {
        Self { path }
    }
}
