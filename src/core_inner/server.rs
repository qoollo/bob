use super::prelude::*;

#[derive(Clone)]
pub struct BobSrv {
    pub grinder: std::sync::Arc<Grinder>,
}

impl BobSrv {
    pub fn run_api_server(&self, port: u16) {
        api::http::spawn(self.clone(), port);
    }

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

type ApiResult<T> = Result<Response<T>, Status>;
// type PutFuture = Result<Response<OpStatus>, Status>;
// type GetFuture = Result<Response<Blob>, Status>;
// type PingFuture = Result<Response<Null>, Status>;

#[tonic::async_trait]
impl BobApi for BobSrv {
    async fn put(&self, req: Request<PutRequest>) -> ApiResult<OpStatus> {
        let sw = Stopwatch::start_new();
        let put_request = req.into_inner();

        if Self::put_is_valid(&put_request) {
            let key = BobKey {
                key: put_request
                    .key
                    .clone()
                    .map(|blob_key| blob_key.key)
                    .expect("get key from request"),
            };
            let blob = put_request.data.clone().expect("get data from request");
            let data = BobData::new(blob.data, BobMeta::new(blob.meta.expect("get blob meta")));

            trace!("PUT[{}] data size: {}", key, data.data.len());
            let put_result = self
                .grinder
                .put(key, data, BobOptions::new_put(put_request.options))
                .await;
            let elapsed = sw.elapsed_ms();
            put_result
                .map(|back_res| {
                    debug!("PUT[{}]-OK local:{:?} ok dt: {}ms", key, back_res, elapsed);
                    Response::new(OpStatus { error: None })
                })
                .map_err(|e| {
                    error!("PUT[{}]-ERR dt: {}ms {:?}", key, elapsed, e);
                    e.into()
                })
        } else {
            warn!("PUT[-] invalid arguments - key and data is mandatory");
            Err(Status::new(
                Code::InvalidArgument,
                "Key and data is mandatory",
            ))
        }
    }

    async fn get(&self, req: Request<GetRequest>) -> ApiResult<Blob> {
        let sw = Stopwatch::start_new();
        let get_req = req.into_inner();
        if !Self::get_is_valid(&get_req) {
            warn!("GET[-] invalid arguments - key is mandatory");
            Err(Status::new(Code::InvalidArgument, "Key is mandatory"))
        } else {
            let key = BobKey {
                key: get_req.clone().key.expect("get key from request").key,
            };

            let grinder = self.grinder.clone();
            let options = BobOptions::new_get(get_req.options);
            let get_res = grinder
                .get(key, &options)
                .await
                .map_err::<Status, _>(|e| e.into())?;

            let elapsed = sw.elapsed_ms();
            debug!("GET[{}]-OK dt: {}ms", key, elapsed);
            Ok(Response::new(Blob {
                data: get_res.data.data,
                meta: Some(BlobMeta {
                    timestamp: get_res.data.meta.timestamp,
                }),
            }))
        }
    }

    async fn ping(&self, _request: Request<Null>) -> ApiResult<Null> {
        debug!("PING");
        Ok(Response::new(Null {}))
    }

    async fn exist(&self, req: Request<ExistRequest>) -> ApiResult<ExistResponse> {
        let sw = Stopwatch::start_new();
        let req = req.into_inner();
        let grinder = self.grinder.clone();
        let keys = req
            .keys
            .into_iter()
            .map(|k| BobKey { key: k.key })
            .collect::<Vec<_>>();
        let options = BobOptions::new_get(req.options);
        let exist_res = tokio::task::spawn_blocking(move || {
            futures::executor::block_on(
                grinder
                    .exist(&keys, &options)
                    .map_err::<Status, _>(|e| e.into()),
            )
        })
        .await
        .unwrap_or_else(|_| Err(Status::internal("spawn_blocking failed")))?;

        let elapsed = sw.elapsed();
        debug!("EXISTS-OK dt: {:?}", elapsed);
        Ok(Response::new(ExistResponse {
            exist: exist_res.exist,
        }))
    }
}
