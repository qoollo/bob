use crate::core::{BackendStorage, Operation};
use anyhow::Result as AnyResult;
use bob_common::{
    data::{BobData, BobKey, BobMeta},
    error::Error,
};
use std::fmt::Debug;

#[derive(Clone, Debug)]
pub struct StubBackend {}

#[async_trait]
impl BackendStorage for StubBackend {
    async fn run_backend(&self) -> AnyResult<()> {
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
}
