use std::{path::{PathBuf, Path}, collections::HashSet, os::unix::fs::MetadataExt};

use bob_common::core_types::DiskPath;
use libc::statvfs;
use sysinfo::{System, SystemExt, DiskExt};

use super::DiskSpaceMetrics;

pub(super) fn collect_used_disks(disks: &[DiskPath]) -> HashSet<PathBuf> {
    System::new_all()
        .disks()
        .iter()
        .filter_map(|d| {
            let path = d.mount_point();
            disks
                .iter()
                .find(move |dp| {
                    let dp_md = Path::new(dp.path())
                        .metadata()
                        .expect("Can't get metadata from OS");
                    let p_md = path.metadata().expect("Can't get metadata from OS");
                    p_md.dev() == dp_md.dev()
                })
                .map(|_| {
                    let diskpath = path.to_str().expect("Not UTF-8").to_owned();
                    PathBuf::from(diskpath)
                })
        })
        .collect()
}

fn to_cpath(path: &Path) -> Vec<u8> {
    use std::{ffi::OsStr, os::unix::ffi::OsStrExt};

    let path_os: &OsStr = path.as_ref();
    let mut cpath = path_os.as_bytes().to_vec();
    cpath.push(0);
    cpath
}

fn statvfs_wrap(mount_point: &Vec<u8>) -> Option<statvfs> {
    unsafe {
        let mut stat: statvfs = std::mem::zeroed();
        if statvfs(mount_point.as_ptr() as *const _, &mut stat) == 0 {
            Some(stat)
        } else {
            None
        }
    }
}

// FIXME: maybe it's better to cache needed disks, but I am not sure, that they would be
// refreshed, if I clone them
// NOTE: HashMap contains only needed mount points of used disks, so it won't be really big,
// but maybe it's more efficient to store disks (instead of mount_points) and update them one by one

pub(super) fn space(disks: &HashSet<PathBuf>) -> DiskSpaceMetrics {
    let mut total = 0;
    let mut used = 0;
    let mut free = 0;

    for mount_point in disks {
        let cm_p = to_cpath(mount_point.as_path());
        let stat = statvfs_wrap(&cm_p);
        if let Some(stat) = stat {
            let bsize = stat.f_bsize as u64;
            let blocks = stat.f_blocks as u64;
            let bavail = stat.f_bavail as u64;
            let bfree = stat.f_bfree as u64;
            total += bsize * blocks;
            free += bsize * bavail;
            used += (blocks - bfree) * bsize;
        }
    }

    DiskSpaceMetrics {
        total_space: total,
        used_space: used,
        free_space: free,
    }
}
