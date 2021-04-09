use crate::prelude::*;

use crate::core::{BackendStorage, Operation, BACKEND_STARTED, BACKEND_STARTING};

#[derive(Clone, Debug)]
pub struct StubBackend {}

#[async_trait]
impl BackendStorage for StubBackend {
    async fn run(&self) -> AnyResult<()> {
        gauge!(BACKEND_STATE, BACKEND_STARTING);
        gauge!(BACKEND_STATE, BACKEND_STARTED);
        Ok(())
    }

    async fn put(&self, _operation: Operation, key: BobKey, data: BobData) -> Result<(), Error> {
        debug!(
            "PUT[{}]: hi from backend, timestamp: {:?}",
            key,
            data.meta()
        );
        Ok(())
    }

    async fn put_alien(
        &self,
        _operation: Operation,
        key: BobKey,
        data: BobData,
    ) -> Result<(), Error> {
        debug!(
            "PUT[{}]: hi from backend, timestamp: {:?}",
            key,
            data.meta()
        );
        Ok(())
    }

    async fn get(&self, _operation: Operation, key: BobKey) -> Result<BobData, Error> {
        debug!("GET[{}]: hi from backend", key);
        Ok(BobData::new(vec![0], BobMeta::stub()))
    }

    async fn get_alien(&self, operation: Operation, key: BobKey) -> Result<BobData, Error> {
        debug!("GET[{}]: hi from backend", key);
        self.get(operation, key).await
    }

    async fn exist(&self, _operation: Operation, _keys: &[BobKey]) -> Result<Vec<bool>, Error> {
        debug!("EXIST: hi from backend");
        Ok(vec![])
    }

    async fn exist_alien(
        &self,
        _operation: Operation,
        _keys: &[BobKey],
    ) -> Result<Vec<bool>, Error> {
        debug!("EXIST: hi from backend");
        Ok(vec![])
    }

    async fn shutdown(&self) {}

    async fn index_memory(&self) -> usize {
        0
    }

    async fn blobs_count(&self) -> (usize, usize) {
        (0, 0)
    }
}
