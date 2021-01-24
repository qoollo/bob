use super::{disk_controller::DiskController, holder::PearlState, prelude::Key, PearlStorage};
use crate::prelude::Result;
use futures::Future;

#[derive(Clone, Debug)]
pub(crate) struct PearlSync {
    disk_controller: DiskController,
    state: PearlState,
    start_time_test: u8,
}
impl PearlSync {
    pub(crate) fn new() -> Self {
        Self {
            disk_controller: DiskController::new(),
            state: PearlState::Initializing,
            start_time_test: 0,
        }
    }

    #[inline]
    pub fn read(&self, key: Key) -> impl Future<Output = Result<Vec<u8>>> + '_ {
        self.disk_controller.read(key)
    }

    #[inline]
    pub fn write(&self, key: Key, value: Vec<u8>) -> impl Future<Output = Result<()>> + '_ {
        self.disk_controller.write(key, value)
    }

    #[inline]
    pub fn contains(&self, key: Key) -> impl Future<Output = Result<bool>> + '_ {
        self.disk_controller.contains(key)
    }

    #[inline]
    pub fn close_active_blob(&self) -> impl Future<Output = ()> + '_ {
        self.disk_controller.close_active_blob()
    }

    #[inline]
    pub fn close(&self) -> impl Future<Output = Result<()>> {
        self.disk_controller.close()
    }

    #[inline]
    pub fn records_count(&self) -> impl Future<Output = usize> + '_ {
        self.disk_controller.records_count()
    }

    pub async fn active_blob_records_count(&self) -> usize {
        self.disk_controller
            .records_count_in_active_blob()
            .await
            .unwrap_or_default()
    }

    #[inline]
    pub fn blobs_count(&self) -> impl Future<Output = usize> + '_ {
        self.disk_controller.blobs_count()
    }

    #[inline]
    pub(crate) fn ready(&mut self) {
        self.set_state(PearlState::Normal);
    }

    #[inline]
    pub(crate) fn init(&mut self) {
        self.set_state(PearlState::Initializing);
    }

    #[inline]
    pub(crate) fn is_ready(&self) -> bool {
        self.state == PearlState::Normal
    }

    #[inline]
    pub(crate) fn is_reinit(&self) -> bool {
        self.state == PearlState::Initializing
    }

    #[inline]
    pub(crate) fn set_state(&mut self, state: PearlState) {
        self.state = state;
    }

    #[inline]
    pub(crate) fn set(&mut self, storage: PearlStorage) {
        self.disk_controller.set(storage);
        self.start_time_test += 1;
    }

    #[inline]
    pub(crate) fn index_memory(&self) -> impl Future<Output = usize> + '_ {
        self.disk_controller.index_memory()
    }
}
