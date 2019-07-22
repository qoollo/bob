use crate::api::grpc::{server, Blob, BlobMeta, GetRequest, Null, OpStatus, PutRequest};

use crate::core::{
    data::{BobData, BobKey, BobMeta, BobOptions},
    grinder::{Error, Grinder},
    bob_client::BobClientFactory,
};
use futures::{
    future,
    future::{err, ok},
    Future,
};
use stopwatch::Stopwatch;
use tower_grpc::{Request, Response};

use futures03::future::{FutureExt, TryFutureExt};
use futures03::task::Spawn;

#[derive(Clone)]
pub struct BobSrv {
    pub grinder: std::sync::Arc<Grinder>,
}

impl BobSrv {
    pub async fn run_backend(&self) -> Result<(), String> {
        self.grinder.run_backend().await
    }

    pub async fn get_periodic_tasks<S>(
        &self,
        client_factory: BobClientFactory,
        spawner: S,
    ) -> Result<(), ()>
    where
        S: Spawn + Clone + Send + 'static + Unpin + Sync,
    {
        self.grinder.get_periodic_tasks(client_factory, spawner).await
    }

    fn put_is_valid(req: &PutRequest) -> bool {
        req.key.is_some() && req.data.is_some()
    }

    fn get_is_valid(req: &GetRequest) -> bool {
        req.key.is_some()
    }
}

impl server::BobApi for BobSrv {
    type PutFuture = Box<dyn Future<Item = Response<OpStatus>, Error = tower_grpc::Status> + Send>;
    type GetFuture = Box<dyn Future<Item = Response<Blob>, Error = tower_grpc::Status> + Send>;
    type PingFuture = future::FutureResult<Response<Null>, tower_grpc::Status>;

    fn put(&mut self, req: Request<PutRequest>) -> Self::PutFuture {
        let sw = Stopwatch::start_new();
        let param = req.into_inner();

        if !Self::put_is_valid(&param) {
            warn!("PUT[-] invalid arguments - key and data is mandatory");
            Box::new(future::err(tower_grpc::Status::new(
                tower_grpc::Code::InvalidArgument,
                "Key and data is mandatory",
            )))
        } else {
            let key = BobKey {
                key: param.clone().key.unwrap().key,
            };
            let blob = param.clone().data.unwrap();
            let data = BobData::new(blob.data, BobMeta::new(blob.meta.unwrap()));

            trace!("PUT[{}] data size: {}", key, data.data.len());
            let grinder = self.grinder.clone();
            let q = async move {
                grinder
                    .put(key, data, {
                        let mut opts: BobOptions = Default::default();
                        if let Some(vopts) = param.options.as_ref() {
                            if vopts.force_node {
                                opts |= BobOptions::FORCE_NODE;
                            }
                        }
                        opts
                    })
                    .await
            };
            Box::new(q.boxed().compat().then(move |r| {
                let elapsed = sw.elapsed_ms();
                match r {
                    Ok(r_ok) => {
                        debug!("PUT[{}]-OK local:{:?} ok dt: {}ms", key, r_ok, elapsed);
                        ok(Response::new(OpStatus { error: None }))
                    }
                    Err(r_err) => {
                        error!("PUT[{}]-ERR dt: {}ms {:?}", key, elapsed, r_err);
                        err(tower_grpc::Status::new(
                            tower_grpc::Code::Internal,
                            format!("Failed to write {:?}", r_err),
                        ))
                    }
                }
            }))
        }
    }

    fn get(&mut self, req: Request<GetRequest>) -> Self::GetFuture {
        let sw = Stopwatch::start_new();
        let param = req.into_inner();
        if !Self::get_is_valid(&param) {
            warn!("GET[-] invalid arguments - key is mandatory");
            Box::new(future::err(tower_grpc::Status::new(
                tower_grpc::Code::InvalidArgument,
                "Key is mandatory",
            )))
        } else {
            let key = BobKey {
                key: param.clone().key.unwrap().key,
            };

            let grinder = self.grinder.clone();
            let q = async move {
                grinder
                    .get(key, {
                        let mut opts: BobOptions = Default::default();
                        if let Some(vopts) = param.options.as_ref() {
                            if vopts.force_node {
                                opts |= BobOptions::FORCE_NODE;
                            }
                        }
                        opts
                    })
                    .await
            };
            Box::new(q.boxed().compat().then(move |r| {
                let elapsed = sw.elapsed_ms();
                match r {
                    Ok(r_ok) => {
                        debug!("GET[{}]-OK dt: {}ms", key, elapsed);
                        ok(Response::new(Blob {
                            data: r_ok.data.data,
                            meta: Some(BlobMeta {
                                timestamp: r_ok.data.meta.timestamp,
                            }),
                        }))
                    }
                    Err(r_err) => {
                        error!(
                            "GET[{}]-ERR  dt: {}ms {:?}",
                            key,
                            // r_err.is_local(),
                            elapsed,
                            r_err
                        );
                        let err = match r_err.error() {
                            Error::NotFound => tower_grpc::Status::new(
                                tower_grpc::Code::NotFound,
                                format!("[bob] Can't find record with key {}", key),
                            ),
                            _ => tower_grpc::Status::new(
                                //TODO add error description
                                tower_grpc::Code::Unknown,
                                "[bob] Some error",
                            ),
                        };
                        future::err(err)
                    }
                }
            }))
        }
    }

    fn ping(&mut self, _request: Request<Null>) -> Self::PingFuture {
        debug!("PING");
        let response = Response::new(Null {});
        future::ok(response)
    }
}
