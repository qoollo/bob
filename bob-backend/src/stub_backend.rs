use crate::prelude::*;

use crate::core::{BackendStorage, MetricsProducer, Operation};

#[derive(Clone, Debug)]
pub struct StubBackend {}

#[async_trait]
impl MetricsProducer for StubBackend {}

#[async_trait]
impl BackendStorage for StubBackend {
    async fn run_backend(&self) -> AnyResult<()> {
        Ok(())
    }

    async fn put(&self, _operation: Operation, key: BobKey, data: &BobData) -> Result<(), Error> {
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
        data: &BobData,
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

    async fn delete(&self, _op: Operation, _key: BobKey) -> Result<u64, Error> {
        Ok(0)
    }

    async fn delete_alien(&self, _op: Operation, _key: BobKey) -> Result<u64, Error> {
        Ok(0)
    }
}
