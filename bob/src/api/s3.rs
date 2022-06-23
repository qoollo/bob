use std::{convert::TryInto, str::FromStr};

use super::{infer_data_type, AuthError, DataKey, StatusExt};
use crate::server::Server as BobServer;
use axum::{
    body::{boxed, BoxBody, Empty, Full},
    extract::{Extension, FromRequest, Path, RequestParts},
    response::{IntoResponse, Response},
    routing::{get, put, MethodRouter},
};

use bob_access::{Authenticator, CredentialsHolder};
use bob_common::{
    data::{BobData, BobKey, BobMeta, BobOptions},
    error::Error,
};
use bytes::Bytes;
use chrono::DateTime;
use http::StatusCode;

#[derive(Debug)]
pub enum StatusS3 {
    StatusExt(StatusExt),
    Status(StatusCode),
}

impl From<StatusExt> for StatusS3 {
    fn from(inner: StatusExt) -> Self {
        Self::StatusExt(inner)
    }
}

impl From<Error> for StatusS3 {
    fn from(err: Error) -> Self {
        StatusExt::from(err).into()
    }
}

impl IntoResponse for StatusS3 {
    fn into_response(self) -> Response<BoxBody> {
        let status = match self {
            Self::StatusExt(status_ext) => status_ext.status,
            Self::Status(status) => status,
        };
        Response::builder()
            .status(status)
            .body(boxed(Empty::new()))
            .expect("failed to set empty body for response")
    }
}

impl From<AuthError> for StatusS3 {
    fn from(err: AuthError) -> Self {
        Self::StatusExt(err.into())
    }
}

pub(crate) fn routes<A>() -> Vec<(&'static str, MethodRouter)>
where
    A: Authenticator + Send + Sync + 'static,
{
    vec![
        ("/s3/default/:key", get(get_object::<A>)),
        ("/s3/default/:key", put(put_object::<A>)),
    ]
}

#[derive(Debug, Default)]
struct GetObjectHeaders {
    content_type: Option<String>,
    if_modified_since: Option<u64>,
    if_unmodified_since: Option<u64>,
}

#[async_trait]
impl<B> FromRequest<B> for GetObjectHeaders
where
    B: Send,
{
    type Rejection = StatusS3;
    async fn from_request(request: &mut RequestParts<B>) -> Result<Self, Self::Rejection> {
        let headers = request
            .headers()
            .expect("headers removed by another extractor");

        let content_type = headers
            .get("response-content-type")
            .and_then(|x| x.to_str().map(|s| s.to_string()).ok());
        let if_modified_since = headers
            .get("If-Modified-Since")
            .and_then(|x| {
                let s = x.to_str().expect("failed to convert header to str");
                DateTime::parse_from_rfc2822(s).ok()
            })
            .and_then(|x| x.timestamp().try_into().ok());
        let if_unmodified_since = headers
            .get("If-Unmodified-Since")
            .and_then(|x| {
                let s = x.to_str().expect("failed to convert header to str");
                DateTime::parse_from_rfc2822(s).ok()
            })
            .and_then(|x| x.timestamp().try_into().ok());
        let headers = GetObjectHeaders {
            content_type,
            if_modified_since,
            if_unmodified_since,
        };
        Ok(headers)
    }
}

#[derive(Debug)]
struct GetObjectOutput {
    data: BobData,
    content_type: String,
}

impl IntoResponse for GetObjectOutput {
    fn into_response(self) -> Response {
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", self.content_type)
            .header("Last-Modified", self.data.meta().timestamp().to_string())
            .body(boxed(Full::new(self.data.into_inner().into())))
            .expect("failed to set body")
    }
}

// GET /s3/default/:key
async fn get_object<A>(
    Extension(bob): Extension<&BobServer<A>>,
    Path(key): Path<String>,
    headers: GetObjectHeaders,
    creds: CredentialsHolder<A>,
) -> Result<GetObjectOutput, StatusS3>
where
    A: Authenticator,
{
    if !bob.auth().check_credentials(creds.into_credentials())?.has_rest_read() {
        return Err(AuthError::PermissionDenied.into());
    }
    let key = DataKey::from_str(&key)?.0;
    let opts = BobOptions::new_get(None);
    let data = bob.grinder().get(key, &opts).await?;
    let content_type = headers
        .content_type
        .unwrap_or_else(|| infer_data_type(&data).to_string());
    let last_modified = data.meta().timestamp();
    if let Some(time) = headers.if_modified_since {
        if time > last_modified {
            return Err(StatusS3::Status(StatusCode::NOT_MODIFIED));
        }
    }
    if let Some(time) = headers.if_unmodified_since {
        if time < last_modified {
            return Err(StatusS3::Status(StatusCode::PRECONDITION_FAILED));
        }
    }
    Ok(GetObjectOutput { data, content_type })
}

// PUT /s3/default/:key
async fn put_object<A>(
    Extension(bob): Extension<&BobServer<A>>,
    Path(key): Path<String>,
    body: Bytes,
    headers: CopyObjectHeaders,
    creds: CredentialsHolder<A>,
) -> Result<StatusS3, StatusS3>
where
    A: Authenticator,
{
    if !bob.auth().check_credentials(creds.into_credentials())?.has_rest_write() {
        return Err(AuthError::PermissionDenied.into());
    }
    let key = DataKey::from_str(&key)?.0;
    if headers.is_source_key_set() {
        return copy_object(bob, key, headers).await;
    }
    let data_buf = body.to_vec();
    let data = BobData::new(
        data_buf,
        BobMeta::new(chrono::Local::now().timestamp() as u64),
    );

    let opts = BobOptions::new_put(None);
    bob.grinder().put(key, data, opts).await?;

    Ok(StatusS3::from(StatusExt::from(StatusCode::CREATED)))
}

#[derive(Debug)]
pub(crate) struct CopyObjectHeaders {
    if_modified_since: Option<u64>,
    if_unmodified_since: Option<u64>,
    source_key: Option<DataKey>,
}

impl CopyObjectHeaders {
    fn is_source_key_set(&self) -> bool {
        self.source_key.is_some()
    }
}

#[async_trait]
impl<B> FromRequest<B> for CopyObjectHeaders
where
    B: Send,
{
    type Rejection = StatusS3;
    async fn from_request(request: &mut RequestParts<B>) -> Result<Self, Self::Rejection> {
        let headers = request
            .headers()
            .expect("headers removed by another extractor");
        let source_key = headers.get("x-amz-copy-source").and_then(|x| {
            let key = x.to_str().map(|s| s.to_string()).ok()?;
            DataKey::from_str(&key).ok()
        });
        Ok(CopyObjectHeaders {
            if_modified_since: headers
                .get("If-Modified-Since")
                .and_then(|x| {
                    let s = x.to_str().expect("failed to convert header to str");
                    chrono::DateTime::parse_from_rfc2822(s).ok()
                })
                .and_then(|x| x.timestamp().try_into().ok()),
            if_unmodified_since: headers
                .get("If-Unmodified-Since")
                .and_then(|x| {
                    let s = x.to_str().expect("failed to convert header to str");
                    chrono::DateTime::parse_from_rfc2822(s).ok()
                })
                .and_then(|x| x.timestamp().try_into().ok()),
            source_key,
        })
    }
}

async fn copy_object<A: Authenticator>(
    bob: &BobServer<A>,
    key: BobKey,
    headers: CopyObjectHeaders,
) -> Result<StatusS3, StatusS3> {
    let opts = BobOptions::new_get(None);
    let data = bob.grinder().get(key, &opts).await?;
    let last_modified = data.meta().timestamp();
    if let Some(time) = headers.if_modified_since {
        if time > last_modified {
            return Err(StatusS3::Status(StatusCode::NOT_MODIFIED));
        }
    }
    if let Some(time) = headers.if_unmodified_since {
        if time < last_modified {
            return Err(StatusS3::Status(StatusCode::PRECONDITION_FAILED));
        }
    }
    let data = BobData::new(
        data.into_inner(),
        BobMeta::new(chrono::Local::now().timestamp() as u64),
    );

    let opts = BobOptions::new_put(None);
    bob.grinder().put(key, data, opts).await?;

    Ok(StatusS3::from(StatusExt::from(StatusCode::OK)))
}
