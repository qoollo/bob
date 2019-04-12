use tokio::prelude::Future;

//use futures::future::;
use crate::core::backend::{
    stub_backend::StubBackend, Backend, BackendError, BackendGetResult, BackendResult,
};
use crate::core::data::{BobData, BobError, BobGetResult, BobKey, BobOptions, ClusterResult};
use crate::core::sprinkler::{Sprinkler, SprinklerError, SprinklerResult};
use futures::future::Either;

pub struct Grinder {
    pub backend: StubBackend,
    pub sprinkler: Sprinkler,
}

#[derive(Debug)]
pub enum ServeTypeOk<CT, BT> {
    Cluster(CT),
    Local(BT),
}

impl<CT, BT> ServeTypeOk<CT, BT> {
    pub fn is_cluster(&self) -> bool {
        match *self {
            ServeTypeOk::Cluster(_) => true,
            ServeTypeOk::Local(_) => false,
        }
    }
    pub fn is_local(&self) -> bool {
        !self.is_cluster()
    }
}

#[derive(Debug)]
pub enum ServeTypeError<CT, BT> {
    Cluster(CT),
    Local(BT),
}

impl<CT, BT> ServeTypeError<CT, BT> {
    pub fn is_cluster(&self) -> bool {
        match *self {
            ServeTypeError::Cluster(_) => true,
            ServeTypeError::Local(_) => false,
        }
    }
    pub fn is_local(&self) -> bool {
        !self.is_cluster()
    }
}

impl Grinder {
    pub fn put(
        &self,
        key: BobKey,
        data: BobData,
        opts: BobOptions,
    ) -> impl Future<
        Item = ServeTypeOk<SprinklerResult, BackendResult>,
        Error = ServeTypeError<SprinklerError, BackendError>,
    >
                 + 'static
                 + Send {
        Box::new(if opts.contains(BobOptions::FORCE_NODE) {
            debug!(
                "PUT[{}] flag FORCE_NODE is on - will handle it by local node",
                key
            );
            Either::A(
                self.backend
                    .put(key, data)
                    .map(|r| ServeTypeOk::Local(r))
                    .map_err(|err| ServeTypeError::Local(err)),
            )
        } else {
            debug!("PUT[{}] will route to cluster", key);
            Either::B(
                self.sprinkler
                    .put_clustered(key, data)
                    .map(|r| ServeTypeOk::Cluster(r))
                    .map_err(|err| ServeTypeError::Cluster(err)),
            )
        })
    }

    pub fn get(
        &self,
        key: BobKey,
        opts: BobOptions,
    ) -> impl Future<
        Item = ServeTypeOk<ClusterResult<BobGetResult>, BackendGetResult>,
        Error = ServeTypeError<BobError, BackendError>,
    >
                 + 'static
                 + Send {
        Box::new(if opts.contains(BobOptions::FORCE_NODE) {
            debug!(
                "GET[{}] flag FORCE_NODE is on - will handle it by local node",
                key
            );
            Either::A(
                self.backend
                    .get(key)
                    .map(|r| ServeTypeOk::Local(r))
                    .map_err(|err| ServeTypeError::Local(err)),
            )
        } else {
            debug!("GET[{}] will route to cluster", key);
            Either::B(
                self.sprinkler
                    .get_clustered(key)
                    .map(|r| ServeTypeOk::Cluster(r))
                    .map_err(|err| ServeTypeError::Cluster(err)),
            )
        })
    }

    pub fn get_periodic_tasks(
        &self,
        ex: tokio::runtime::TaskExecutor,
    ) -> Box<impl Future<Item = (), Error = ()>> {
        self.sprinkler.get_periodic_tasks(ex)
    }
}
