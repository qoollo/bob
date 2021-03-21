use super::*;
use chrono::Local;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

#[derive(Clone, Debug)]
pub(crate) struct DisksEventsLogger {
    fd: Arc<RwLock<File>>,
}

impl DisksEventsLogger {
    pub(crate) async fn new(filename: impl AsRef<Path>) -> Self {
        let fd = if filename.as_ref().exists() {
            OpenOptions::new()
                .append(true)
                .open(filename)
                .await
                .expect("Can't open log file for disks events")
        } else {
            let mut f = File::create(filename)
                .await
                .expect("Can't create log file for disks events");
            Self::write_header(&mut f)
                .await
                .expect("Failed to write header for DisksEventsLogger");
            f
        };
        let fd = Arc::new(RwLock::new(fd));
        Self { fd }
    }

    async fn write_header(f: &mut File) -> Result<()> {
        f.write_all(b"disk_name;new_state;datetime\n").await?;
        f.sync_all().map_err(|e| e.into()).await
    }

    pub(crate) async fn log(&self, disk_name: &str, event: &str) {
        let cur_time = Local::now();
        let log_msg = format!("{};{};{}\n", disk_name, event, cur_time.format("%+"));
        let mut flock = self.fd.write().await;
        if let Err(e) = flock.write_all(log_msg.as_bytes()).await {
            error!("Can't write disk event!!! (reason: {:?})", e);
        }
        if let Err(e) = flock.sync_data().await {
            error!("Can't sync file with disk events!!! (reason: {:?})", e);
        }
    }
}
