use crate::api::grpc::{server, Blob, BlobMeta, GetRequest, Null, OpStatus, PutRequest};

use crate::core::backend::BackendError;

use crate::core::data::{BobData, BobError, BobKey, BobMeta, BobOptions};
use crate::core::grinder::{Grinder, ServeTypeError, ServeTypeOk};
use futures::future::{err, ok};
use futures::{future, Future};
use stopwatch::Stopwatch;
use tower_grpc::{Request, Response};

#[derive(Clone)]
pub struct BobSrv {
    pub grinder: std::sync::Arc<Grinder>,
}

impl BobSrv {
    pub fn get_periodic_tasks(
        &self,
        ex: tokio::runtime::TaskExecutor,
    ) -> Box<impl Future<Item = (), Error = ()>> {
        self.grinder.get_periodic_tasks(ex)
    }

    fn put_is_valid(req: &PutRequest) -> bool {
        req.key.is_some() && req.data.is_some()
    }

    fn get_is_valid(req: &GetRequest) -> bool {
        req.key.is_some()
    }
}

impl server::BobApi for BobSrv {
    type PutFuture = Box<Future<Item = Response<OpStatus>, Error = tower_grpc::Status> + Send>;
    type GetFuture = Box<Future<Item = Response<Blob>, Error = tower_grpc::Status> + Send>;
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
                key: param.key.unwrap().key,
            };
            let blob = param.data.unwrap();
            let data = BobData {
                data: blob.data,
                meta: BobMeta::new(blob.meta.unwrap()),
            };

            trace!("PUT[{}] data size: {}", key, data.data.len());
            Box::new(
                self.grinder
                    .put(key, data, {
                        let mut opts: BobOptions = Default::default();
                        if let Some(vopts) = param.options.as_ref() {
                            if vopts.force_node {
                                opts |= BobOptions::FORCE_NODE;
                            }
                        }
                        opts
                    })
                    .then(move |r| {
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
                    }),
            )
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
                key: param.key.unwrap().key,
            };

            Box::new(
                self.grinder
                    .get(key, {
                        let mut opts: BobOptions = Default::default();
                        if let Some(vopts) = param.options.as_ref() {
                            if vopts.force_node {
                                opts |= BobOptions::FORCE_NODE;
                            }
                        }
                        opts
                    })
                    .then(move |r| {
                        let elapsed = sw.elapsed_ms();
                        match r {
                            Ok(r_ok) => {
                                debug!(
                                    "GET[{}]-OK local:{} dt: {}ms",
                                    key,
                                    r_ok.is_local(),
                                    elapsed
                                );
                                ok(Response::new(match r_ok {
                                    ServeTypeOk::Cluster(r) => Blob {
                                        data: r.result.data.data,
                                        meta: Some(BlobMeta {
                                            timestamp: r.result.data.meta.timestamp,
                                        }),
                                    },
                                    ServeTypeOk::Local(r) => Blob {
                                        data: r.data.data,
                                        meta: Some(BlobMeta {
                                            timestamp: r.data.meta.timestamp,
                                        }),
                                    },
                                }))
                            }
                            Err(r_err) => {
                                error!(
                                    "GET[{}]-ERR local:{}  dt: {}ms {:?}",
                                    key,
                                    r_err.is_local(),
                                    elapsed,
                                    r_err
                                );
                                let err = match r_err {
                                    ServeTypeError::Cluster(cerr) => match cerr {
                                        BobError::NotFound => tower_grpc::Status::new(
                                            tower_grpc::Code::NotFound,
                                            format!("[cluster] Can't find blob with key {}", key),
                                        ),
                                        _ => tower_grpc::Status::new(
                                            tower_grpc::Code::Unknown,
                                            "[cluster]Some error",
                                        ),
                                    },
                                    ServeTypeError::Local(lerr) => match lerr {
                                        BackendError::NotFound => tower_grpc::Status::new(
                                            tower_grpc::Code::NotFound,
                                            format!("[backend] Can't find blob with key {}", key),
                                        ),
                                        _ => tower_grpc::Status::new(
                                            tower_grpc::Code::Unknown,
                                            "[backend] Some error",
                                        ),
                                    },
                                };
                                future::err(err)
                            }
                        }
                    }),
            )
        }
    }

    fn ping(&mut self, _request: Request<Null>) -> Self::PingFuture {
        debug!("PING");
        let response = Response::new(Null {});
        future::ok(response)
    }
}
