use super::prelude::*;

#[derive(Clone)]
pub struct BobSrv {
    pub grinder: std::sync::Arc<Grinder>,
}

impl BobSrv {
    pub async fn run_backend(&self) -> Result<(), BackendError> {
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
        self.grinder
            .get_periodic_tasks(client_factory, spawner)
            .await
    }

    fn put_is_valid(req: &PutRequest) -> bool {
        req.key.is_some() && req.data.is_some()
    }

    fn get_is_valid(req: &GetRequest) -> bool {
        req.key.is_some()
    }
}

type PutFuture = Box<dyn Future<Output = Result<Response<OpStatus>, Status>> + Send>;
type GetFuture = Box<dyn Future<Output = Result<Response<Blob>, Status>> + Send>;
type PingFuture = future::Ready<Result<Response<Null>, Status>>;

impl BobApi for BobSrv {
    fn put(&self, req: Request<PutRequest>) -> PutFuture {
        // let sw = Stopwatch::start_new();
        // let param = req.into_inner();

        // if !Self::put_is_valid(&param) {
        //     warn!("PUT[-] invalid arguments - key and data is mandatory");
        //     Box::new(future::err(Status::new(
        //         Code::InvalidArgument,
        //         "Key and data is mandatory",
        //     )))
        // } else {
        //     let key = BobKey {
        //         key: param.clone().key.unwrap().key,
        //     };
        //     let blob = param.clone().data.unwrap();
        //     let data = BobData::new(blob.data, BobMeta::new(blob.meta.unwrap()));

        //     trace!("PUT[{}] data size: {}", key, data.data.len());
        //     let grinder = self.grinder.clone();
        //     let q = async move {
        //         grinder
        //             .put(key, data, BobOptions::new_put(param.options))
        //             .await
        //     };
        //     Box::new(q.boxed().then(move |r| {
        //         let elapsed = sw.elapsed_ms();
        //         match r {
        //             Ok(r_ok) => {
        //                 debug!("PUT[{}]-OK local:{:?} ok dt: {}ms", key, r_ok, elapsed);
        //                 future::ok(Response::new(OpStatus { error: None }))
        //             }
        //             Err(r_err) => {
        //                 error!("PUT[{}]-ERR dt: {}ms {:?}", key, elapsed, r_err);
        //                 future::err(r_err.into())
        //             }
        //         }
        //     }))
        // }
        unimplemented!()
    }

    fn get(&mut self, req: Request<GetRequest>) -> GetFuture {
        let sw = Stopwatch::start_new();
        let param = req.into_inner();
        if !Self::get_is_valid(&param) {
            warn!("GET[-] invalid arguments - key is mandatory");
            Box::new(future::err(Status::new(
                Code::InvalidArgument,
                "Key is mandatory",
            )))
        } else {
            let key = BobKey {
                key: param.clone().key.unwrap().key,
            };

            let grinder = self.grinder.clone();
            let q = async move { grinder.get(key, BobOptions::new_get(param.options)).await };
            Box::new(q.boxed().then(move |r| {
                let elapsed = sw.elapsed_ms();
                match r {
                    Ok(r_ok) => {
                        debug!("GET[{}]-OK dt: {}ms", key, elapsed);
                        future::ok(Response::new(Blob {
                            data: r_ok.data.data,
                            meta: Some(BlobMeta {
                                timestamp: r_ok.data.meta.timestamp,
                            }),
                        }))
                    }
                    Err(r_err) => future::err(r_err.into()),
                }
            }))
        }
    }

    fn ping(&mut self, _request: Request<Null>) -> PingFuture {
        debug!("PING");
        let response = Response::new(Null {});
        future::ok(response)
    }
}
