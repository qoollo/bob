use super::prelude::*;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender};

const CHECK_INTERVAL: Duration = Duration::from_secs(2);
const BUFFER_SIZE: usize = 1024; // 1 Kb

enum Event {
    NewPath(PathBuf),
    ResetPath,
}

impl Event {
    fn new_path(path: PathBuf) -> Self {
        Event::NewPath(path)
    }

    fn reset_path() -> Self {
        Event::ResetPath
    }
}

#[derive(Debug, Clone)]
pub struct DiskController {
    storage: Option<PearlStorage>,
    available: Arc<AtomicBool>,
    sender: Sender<Event>,
}

impl DiskController {
    pub fn new() -> Self {
        let (tx, rx) = channel(BUFFER_SIZE);
        let available = Arc::new(AtomicBool::new(true));
        let available_copy = available.clone();
        tokio::spawn(async move { Self::monitor_task(available_copy, rx) });
        Self {
            storage: None,
            available,
            sender: tx,
        }
    }

    async fn monitor_task(availability: Arc<AtomicBool>, mut rx: Receiver<Event>) {
        let mut path = None;
        loop {
            while let Ok(event) = timeout(CHECK_INTERVAL, rx.recv()).await {
                match event {
                    Some(Event::NewPath(new_path)) => path = Some(new_path),
                    Some(Event::ResetPath) => {
                        // we should set it to true, because if it was false for previous storage, monitor
                        // wouldn't be able to change it to true, cause we reset path (so it will be always
                        // false and write operations won't be performed)
                        availability.store(true, Ordering::SeqCst);
                        path = None;
                    }
                    None => {
                        warn!("Disk controller was dropped, linked monitor task either returns");
                        return;
                    }
                }
            }

            if let Some(ref path) = path {
                availability.store(path.exists(), Ordering::SeqCst);
            }
        }
    }

    #[inline]
    pub fn read(&self, key: Key) -> impl Future<Output = Result<Vec<u8>>> + '_ {
        self.storage.as_ref().unwrap().read(key)
    }

    fn writable(&self) -> bool {
        self.available.load(Ordering::SeqCst)
    }

    // @TODO check if there is an efficient way to avoid mutable borrowing
    pub async fn write(&self, key: Key, value: Vec<u8>) -> Result<()> {
        if let Some(storage) = self.storage.as_ref() {
            if !self.writable() {
                return Err(Error::disk_is_unavailable().into());
            }
            match storage.write(key, value).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    if let Some(pearl_err) = e.downcast_ref::<PearlError>() {
                        error!("pearl error: {:#}", pearl_err);
                        // on pearl level WorkDirUnavailable in further version will contain PathBuf,
                        // so we should use it without cast of string to Path[Buf]
                        if let ErrorKind::WorkDirUnavailable(path) = pearl_err.kind() {
                            self.available.store(false, Ordering::SeqCst);
                            if let Err(e) = self.sender.send(Event::new_path(path.into())).await {
                                error!("Receiver from monitor task is dropped (reason: {})", e);
                            }
                        }
                        Err(e)
                    } else {
                        error!("not pearl error: {:#}", e);
                        Err(e)
                    }
                }
            }
        } else {
            error!("Write attempt on disk_controller without storage");
            Err(Error::storage("No storage for disk_controller").into())
        }
    }

    #[inline]
    pub fn contains(&self, key: Key) -> impl Future<Output = Result<bool>> + '_ {
        self.storage.as_ref().unwrap().contains(key)
    }

    #[inline]
    pub fn records_count(&self) -> impl Future<Output = usize> + '_ {
        self.storage.as_ref().unwrap().records_count()
    }

    #[inline]
    pub fn blobs_count(&self) -> impl Future<Output = usize> + '_ {
        self.storage.as_ref().unwrap().blobs_count()
    }

    #[inline]
    pub fn records_count_in_active_blob(&self) -> impl Future<Output = Option<usize>> + '_ {
        self.storage
            .as_ref()
            .unwrap()
            .records_count_in_active_blob()
    }

    #[inline]
    pub fn close_active_blob(&self) -> impl Future<Output = ()> + '_ {
        self.storage.as_ref().unwrap().close_active_blob()
    }

    #[inline]
    pub fn close(&self) -> impl Future<Output = Result<()>> {
        self.storage.clone().unwrap().close()
    }

    #[inline]
    pub fn set(&mut self, storage: PearlStorage) -> Option<PearlStorage> {
        // we can't reach work_dir through public api, so we need to just reset tracked path
        // when we change storage
        if let Err(e) = self.sender.try_send(Event::reset_path()) {
            error!("Failed to send message to monitor task (likely channel buffer is full). Reason: {}", e);
        }
        self.storage.replace(storage)
    }

    pub fn index_memory(&self) -> impl Future<Output = usize> + '_ {
        self.storage.as_ref().unwrap().index_memory()
    }
}

#[tokio::test]
async fn test_disk_controller_write() {
    let mut disk_controller = DiskController::new();
    let mut storage = ::pearl::Builder::new()
        .work_dir("some/irrelevant/path/")
        .blob_file_name_prefix("test")
        .max_blob_size(10_000)
        .max_data_in_blob(10_000)
        .build()
        .unwrap();
    storage.init().await.unwrap();
    disk_controller.set(storage);
    let key = Key::from(13);
    let data = vec![1, 2, 3, 4];
    disk_controller.write(key, data).await.unwrap();
    todo!()
}
