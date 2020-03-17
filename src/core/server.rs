use super::prelude::*;

/// Struct contains `Grinder` and receives incomming GRPC requests
#[derive(Clone, Debug)]
pub struct Server {
    grinder: Arc<Grinder>,
}

impl Server {
    /// Creates new bob server
    #[must_use]
    pub fn new(grinder: Grinder) -> Self {
        Self {
            grinder: Arc::new(grinder),
        }
    }

    pub(crate) fn grinder(&self) -> &Grinder {
        self.grinder.as_ref()
    }

    /// Call to run HTTP API server, not required for normal functioning
    pub fn run_api_server(&self, port: u16) {
        api::http::spawn(self.clone(), port);
    }

    /// Start backend component, required before starting bob service
    /// # Errors
    /// Returns errror if there are any issues with starting backend,
    /// e.g. any fs I/O errors
    pub async fn run_backend(&self) -> Result<(), BackendError> {
        self.grinder.run_backend().await
    }

    /// Spawns background tasks, required before starting bob service
    #[inline]
    pub fn run_periodic_tasks(&self, client_factory: Factory) {
        self.grinder.run_periodic_tasks(client_factory);
    }

    fn put_is_valid(req: &PutRequest) -> bool {
        req.key.is_some() && req.data.is_some()
    }

    fn get_is_valid(req: &GetRequest) -> bool {
        req.key.is_some()
    }
}

fn put_get_inner(req: PutRequest) -> Option<(BlobKey, Blob, i64, Option<PutOptions>)> {
    let key = req.key?;
    let blob = req.data?;
    let timestamp = blob.meta.as_ref()?.timestamp;
    let options = req.options;
    Some((key, blob, timestamp, options))
}

type ApiResult<T> = Result<Response<T>, Status>;

#[tonic::async_trait]
impl BobApi for Server {
    async fn put(&self, req: Request<PutRequest>) -> ApiResult<OpStatus> {
        let sw = Stopwatch::start_new();
        let put_request = req.into_inner();

        if let Some((blob_key, blob, timestamp, options)) = put_get_inner(put_request) {
            let key = blob_key.key;
            let meta = BobMeta::new(timestamp);
            let data = BobData::new(blob.data.clone(), meta);

            trace!("PUT[{}] data size: {}", key, data.inner().len());
            let put_result = self
                .grinder
                .put(key, data, BobOptions::new_put(options))
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
            let key = get_req.clone().key.expect("get key from request").key;

            let grinder = self.grinder.clone();
            let options = BobOptions::new_get(get_req.options);
            let get_res = grinder
                .get(key, &options)
                .await
                .map_err::<Status, _>(|e| e.into())?;

            let elapsed = sw.elapsed_ms();
            debug!("GET[{}]-OK dt: {}ms", key, elapsed);
            Ok(Response::new(Blob {
                meta: Some(BlobMeta {
                    timestamp: get_res.data.meta().timestamp(),
                }),
                data: get_res.data.into_inner(),
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
        let keys = req.keys.into_iter().map(|k| k.key).collect::<Vec<_>>();
        let options = BobOptions::new_get(req.options);
        let exist_res = tokio::task::spawn_blocking(move || {
            futures::executor::block_on(
                grinder
                    .exist(&keys, &options)
                    .map_err::<Status, _>(|e| e.into()),
            )
        })
        .await
        .unwrap_or(Err(Status::internal("spawn_blocking failed")))?;

        let elapsed = sw.elapsed();
        debug!("EXISTS-OK dt: {:?}", elapsed);
        Ok(Response::new(ExistResponse {
            exist: exist_res.exist,
        }))
    }
}
