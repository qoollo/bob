use std::net::IpAddr;

use bob_access::{Authenticator, CredentialsHolder};
use bytes::Bytes;
use tokio::{runtime::Handle, task::block_in_place};

use crate::prelude::*;

use super::grinder::Grinder;
use bob_common::{configs::node::TLSConfig, metrics::SharedMetricsSnapshot};

/// Struct contains `Grinder` and receives incomming GRPC requests
#[derive(Clone, Debug)]
pub struct Server<A: Authenticator> {
    handle: Handle,
    grinder: Arc<Grinder>,
    shared_metrics: SharedMetricsSnapshot,
    auth: A,
}

impl<A> Server<A>
where
    A: Authenticator,
{
    /// Creates new bob server
    #[must_use]
    pub fn new(
        grinder: Grinder,
        handle: Handle,
        shared_metrics: SharedMetricsSnapshot,
        auth: A,
    ) -> Self {
        Self {
            handle,
            grinder: Arc::new(grinder),
            shared_metrics,
            auth,
        }
    }

    pub fn block_on<F: Future>(&self, f: F) -> F::Output {
        block_in_place(|| self.handle.block_on(f))
    }

    pub(crate) fn grinder(&self) -> &Grinder {
        self.grinder.as_ref()
    }

    pub(crate) fn metrics(&self) -> &SharedMetricsSnapshot {
        &self.shared_metrics
    }

    /// Call to run HTTP API server, not required for normal functioning
    pub async fn run_api_server(&self, address: IpAddr, port: u16, tls_config: &Option<TLSConfig>) {
        crate::api::spawn(self.clone(), address, port, tls_config).await;
    }

    /// Start backend component, required before starting bob service
    /// # Errors
    /// Returns errror if there are any issues with starting backend,
    /// e.g. any fs I/O errors
    pub async fn run_backend(&self) -> Result<(), Error> {
        self.grinder
            .run_backend()
            .await
            .map_err(|e| Error::failed(format!("{:#?}", e)))
    }

    /// Spawns background tasks, required before starting bob service
    #[inline]
    pub fn run_periodic_tasks(&self, client_factory: Factory) {
        self.grinder.run_periodic_tasks(client_factory);
    }

    /// Gracefully shutdowns bob
    pub async fn shutdown(&self) {
        let backend = self.grinder.backend().clone();
        backend.shutdown().await;
    }

    pub fn auth(&self) -> &A {
        &self.auth
    }
}

fn put_extract(req: PutRequest) -> Option<(BobKey, Bytes, u64, Option<PutOptions>)> {
    let key = req.key?.key;
    let blob = req.data?;
    let timestamp = blob.meta.as_ref()?.timestamp;
    let options = req.options;
    Some((key.into(), blob.data, timestamp, options))
}

fn get_extract(req: GetRequest) -> Option<(BobKey, Option<GetOptions>)> {
    let key = req.key?.key;
    let options = req.options;
    Some((key.into(), options))
}

fn delete_extract(req: DeleteRequest) -> Option<(BobKey, u64, Option<DeleteOptions>)> {
    let key = req.key?.key;
    let timestamp = req.meta.as_ref()?.timestamp;
    let options = req.options;
    Some((key.into(), timestamp, options))
}

type ApiResult<T> = Result<Response<T>, Status>;

#[tonic::async_trait]
impl<A> BobApi for Server<A>
where
    A: Authenticator,
{
    async fn put(&self, req: Request<PutRequest>) -> ApiResult<OpStatus> {
        let creds: CredentialsHolder<A> = (&req).into();
        if !self.auth.check_credentials_grpc(creds.into())?.has_write() {
            return Err(Status::permission_denied("WRITE permission required"));
        }
        trace!("- - - - - SERVER PUT START - - - - -");
        let sw = Stopwatch::start_new();
        trace!(
            "process incoming put request /{:.3}ms/",
            sw.elapsed().as_secs_f64() * 1000.0
        );
        let put_request = req.into_inner();
        trace!(
            "convert request into inner, /{:.3}ms/",
            sw.elapsed().as_secs_f64() * 1000.0
        );

        if let Some((key, inner, timestamp, options)) = put_extract(put_request) {
            trace!(
                "extract params from request, /{:.3}ms/",
                sw.elapsed().as_secs_f64() * 1000.0
            );
            let meta = BobMeta::new(timestamp);
            let data = BobData::new(inner, meta);

            trace!(
                "PUT[{}] data size: {}, /{:.3}ms/",
                key,
                data.inner().len(),
                sw.elapsed().as_secs_f64() * 1000.0
            );
            let put_result = self
                .grinder
                .put(key, &data, BobPutOptions::from_grpc(options))
                .await;
            trace!(
                "grinder processed put request, /{:.3}ms/",
                sw.elapsed().as_secs_f64() * 1000.0
            );
            let elapsed = sw.elapsed_ms();
            trace!(
                "- - - - - SERVER PUT FINISH - - - - -, /{:.3}ms",
                sw.elapsed().as_secs_f64() * 1000.0
            );
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
                "Key, data and timestamp in data.meta is mandatory",
            ))
        }
    }

    async fn get(&self, req: Request<GetRequest>) -> ApiResult<Blob> {
        let creds: CredentialsHolder<A> = (&req).into();
        if !self.auth.check_credentials_grpc(creds.into())?.has_read() {
            return Err(Status::permission_denied("READ permission required"));
        }
        trace!("- - - - - SERVER GET START - - - - -");
        let sw = Stopwatch::start_new();
        trace!(
            "process incoming get request /{:.3}ms/",
            sw.elapsed().as_secs_f64() * 1000.0
        );
        let get_req = req.into_inner();
        trace!(
            "extract options from request /{:.3}ms/",
            sw.elapsed().as_secs_f64() * 1000.0
        );
        if let Some((key, options)) = get_extract(get_req) {
            trace!(
                "create new bob options /{:.3}ms/",
                sw.elapsed().as_secs_f64() * 1000.0
            );
            let options = BobGetOptions::from_grpc(options);
            trace!(
                "pass request to grinder /{:.3}ms/",
                sw.elapsed().as_secs_f64() * 1000.0
            );
            let get_res = self
                .grinder
                .get(key, &options)
                .await
                .map_err::<Status, _>(|e| e.into())?;
            trace!(
                "grinder finished request processing /{:.3}ms/",
                sw.elapsed().as_secs_f64() * 1000.0
            );
            debug!("GET[{}]-OK dt: {}ms", key, sw.elapsed_ms());
            let meta = Some(BlobMeta {
                timestamp: get_res.meta().timestamp(),
            });
            let data = get_res.into_inner();
            let blob = Blob { data, meta };
            let response = Response::new(blob);
            trace!("- - - - - SERVER GET FINISHED - - - - -");
            Ok(response)
        } else {
            warn!("GET[-] invalid arguments - key is mandatory");
            Err(Status::new(Code::InvalidArgument, "Key is mandatory"))
        }
    }

    async fn ping(&self, r: Request<Null>) -> ApiResult<Null> {
        debug!("PING");
        if let Some(node_name) = r.metadata().get("node_name") {
            if let Ok(name) = node_name.to_str() {
                self.grinder.update_node_connection(name);
            }
        }
        Ok(Response::new(Null {}))
    }

    async fn exist(&self, req: Request<ExistRequest>) -> ApiResult<ExistResponse> {
        let creds: CredentialsHolder<A> = (&req).into();
        if !self.auth.check_credentials_grpc(creds.into())?.has_read() {
            return Err(Status::permission_denied("READ permission required"));
        }
        let sw = Stopwatch::start_new();
        let req = req.into_inner();
        let ExistRequest { keys, options } = req;
        let keys = keys.into_iter().map(|k| k.key.into()).collect::<Vec<_>>();
        let options = BobGetOptions::from_grpc(options);
        let exist = self
            .grinder
            .exist(&keys, &options)
            .await
            .map_err::<Status, _>(|e| e.into())?;
        debug!("EXISTS-OK dt: {:?}", sw.elapsed());
        let response = ExistResponse { exist };
        let response = Response::new(response);
        Ok(response)
    }

    async fn delete(&self, req: Request<DeleteRequest>) -> ApiResult<OpStatus> {
        let creds: CredentialsHolder<A> = (&req).into();
        if !self.auth.check_credentials_grpc(creds.into())?.has_write() {
            return Err(Status::permission_denied("WRITE permission required"));
        }

        let req = req.into_inner();
        if let Some((key, timestamp, options)) = delete_extract(req) {
            trace!("DELETE[{}] request processing started", key);
            let sw = Stopwatch::start_new();
            let delete_result = self.grinder
                .delete(
                    key,
                    &BobMeta::new(timestamp),
                    BobDeleteOptions::from_grpc(options),
                ).await;

            delete_result
                .map(|_| {
                    debug!("DELETE[{}]-OK dt: {:?}", key, sw.elapsed());
                    Response::new(OpStatus { error: None })
                })
                .map_err(|e| {
                    warn!("DELETE[{}]-ERR dt: {:?}, error: {:?}", key, sw.elapsed(), e);
                    e.into()
                })
        } else {
            Err(Status::new(
                Code::InvalidArgument,
                "Key and meta are mandatory",
            ))
        }
    }
}
