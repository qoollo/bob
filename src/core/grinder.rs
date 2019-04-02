use tokio::prelude::Future;

//use futures::future::;
use crate::core::backend::{Backend, BackendError, BackendResult};
use crate::core::data::{BobData, BobKey, BobOptions};
use crate::core::sprinkler::{Sprinkler, SprinklerError, SprinklerResult};
use futures::future::Either;
use futures::Poll;

pub struct Grinder {
    pub backend: Backend,
    pub sprinkler: Sprinkler,
}

pub enum ServeTypeOk {
    Cluster(SprinklerResult),
    Local(BackendResult),
}

pub enum ServeTypeError {
    Cluster(SprinklerError),
    Local(BackendError),
}

pub struct GrinderOk {
    pub is_clustered: bool,
    pub details: ServeTypeOk,
}

impl std::fmt::Display for GrinderOk {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self.details {
            ServeTypeOk::Cluster(detail) => write!(f, "[status= {}]", detail),
            ServeTypeOk::Local(_) => write!(f, "-",),
        }
    }
}

pub struct GrinderError {
    pub is_clustered: bool,
    pub details: ServeTypeError,
}

impl std::fmt::Display for GrinderError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self.details {
            ServeTypeError::Cluster(detail) => write!(f, "[status= {}]", detail),
            ServeTypeError::Local(_) => write!(f, "-",),
        }
    }
}

pub struct GrinderPutResponse<T>(T);

impl<T> Future for GrinderPutResponse<T>
where
    T: Future,
{
    type Item = T::Item;
    type Error = T::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll()
    }
}

impl Grinder {
    pub fn put(
        &self,
        key: BobKey,
        data: BobData,
        opts: BobOptions,
    ) -> impl Future<Item = GrinderOk, Error = GrinderError> + 'static + Send {
        Box::new(if opts.contains(BobOptions::FORCE_NODE) {
            debug!(
                "PUT[{}] flag FORCE_NODE is on - will handle it by local node",
                key
            );
            Either::A(
                self.backend
                    .put(key, data)
                    .map(|r| GrinderOk {
                        is_clustered: false,
                        details: ServeTypeOk::Local(r),
                    })
                    .map_err(|err| GrinderError {
                        is_clustered: false,
                        details: ServeTypeError::Local(err),
                    }),
            )
        } else {
            debug!("PUT[{}] will route to cluster", key);
            Either::B(
                self.sprinkler
                    .put_clustered(key, data)
                    .map(|r| GrinderOk {
                        is_clustered: true,
                        details: ServeTypeOk::Cluster(r),
                    })
                    .map_err(|err| GrinderError {
                        is_clustered: true,
                        details: ServeTypeError::Cluster(err),
                    }),
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
