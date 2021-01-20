use super::PearlStorage;

#[derive(Debug, Clone)]
pub struct DiskController {
    storage: Option<PearlStorage>,
}

impl DiskController {
    pub fn new() -> Self {
        Self { storage: None }
    }

    pub fn storage(&self) -> &PearlStorage {
        self.storage.as_ref().unwrap()
    }

    pub fn set(&mut self, storage: PearlStorage) -> Option<PearlStorage> {
        self.storage.replace(storage)
    }
}
