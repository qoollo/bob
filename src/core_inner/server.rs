use super::prelude::*;

#[derive(Clone)]
pub struct BobSrv {
    pub grinder: std::sync::Arc<Grinder>,
}

impl BobSrv {
    pub async fn run_backend(&self) -> Result<(), BackendError> {
        self.grinder.run_backend().await
    }

    #[inline]
    pub async fn get_periodic_tasks(&self, client_factory: BobClientFactory) -> Result<(), ()> {
        self.grinder.get_periodic_tasks(client_factory).await
    }

    fn put_is_valid(req: &PutRequest) -> bool {
        req.key.is_some() && req.data.is_some()
    }

    fn get_is_valid(req: &GetRequest) -> bool {
        req.key.is_some()
    }
}

type PutFuture = Result<Response<OpStatus>, Status>;
type GetFuture = Result<Response<Blob>, Status>;
type PingFuture = Result<Response<Null>, Status>;

#[tonic::async_trait]
impl BobApi for BobSrv {
    async fn put(&self, req: Request<PutRequest>) -> PutFuture {
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

    async fn get(&self, req: Request<GetRequest>) -> GetFuture {
        let sw = Stopwatch::start_new();
        let param = req.into_inner();
        if !Self::get_is_valid(&param) {
            warn!("GET[-] invalid arguments - key is mandatory");
            // Box::new(future::err(Status::new(
            //     Code::InvalidArgument,
            //     "Key is mandatory",
            // )))
            unimplemented!()
        } else {
            let key = BobKey {
                key: param.clone().key.unwrap().key,
            };

            let grinder = self.grinder.clone();
            let q = async move { grinder.get(key, BobOptions::new_get(param.options)).await };
            // Box::new(q.boxed().then(move |r| {
            //     let elapsed = sw.elapsed_ms();
            //     match r {
            //         Ok(r_ok) => {
            //             debug!("GET[{}]-OK dt: {}ms", key, elapsed);
            //             future::ok(Response::new(Blob {
            //                 data: r_ok.data.data,
            //                 meta: Some(BlobMeta {
            //                     timestamp: r_ok.data.meta.timestamp,
            //                 }),
            //             }))
            //         }
            //         Err(r_err) => future::err(r_err.into()),
            //     }
            // }))
            unimplemented!()
        };
        unimplemented!()
    }

    async fn ping(&self, _request: Request<Null>) -> PingFuture {
        debug!("PING");
        let response = Response::new(Null {});
        // future::ok(response)
        unimplemented!()
    }
}
