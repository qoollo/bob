use std::{convert::TryInto, io::Cursor, str::FromStr};

use super::{infer_data_type, DataKey, StatusExt};
use crate::server::Server as BobServer;
use bob_common::data::{BobData, BobMeta, BobOptions};
use rocket::{
    data::ByteUnit,
    get,
    http::{ContentType, Header, Status},
    put,
    request::{FromRequest, Outcome},
    response,
    response::Responder,
    Data, Request, Response, Route, State,
};

#[derive(Debug)]
pub(crate) enum StatusS3 {
    StatusExt(StatusExt),
    Status(Status),
}

impl From<StatusExt> for StatusS3 {
    fn from(inner: StatusExt) -> Self {
        Self::StatusExt(inner)
    }
}

impl From<std::io::Error> for StatusS3 {
    fn from(err: std::io::Error) -> Self {
        StatusExt::from(err).into()
    }
}

impl From<bob_common::error::Error> for StatusS3 {
    fn from(err: bob_common::error::Error) -> Self {
        StatusExt::from(err).into()
    }
}

impl Responder<'_, 'static> for StatusS3 {
    fn respond_to(self, request: &Request) -> response::Result<'static> {
        match self {
            Self::StatusExt(status_ext) => {
                let resp = status_ext.respond_to(request)?;
                Response::build().status(resp.status()).ok()
            }
            Self::Status(status) => Response::build().status(status).ok(),
        }
    }
}

pub(crate) fn routes() -> impl Into<Vec<Route>> {
    routes![get_object, put_object, copy_object]
}

#[derive(Debug, Default)]
pub(crate) struct GetObjectHeaders {
    content_type: Option<ContentType>,
    if_modified_since: Option<u64>,
    if_unmodified_since: Option<u64>,
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for GetObjectHeaders {
    type Error = StatusS3;
    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        let headers = request.headers();
        Outcome::Success(GetObjectHeaders {
            content_type: headers
                .get_one("response-content-type")
                .and_then(|x| ContentType::from_str(x).ok()),
            if_modified_since: headers
                .get_one("If-Modified-Since")
                .and_then(|x| chrono::DateTime::parse_from_rfc2822(x).ok())
                .and_then(|x| x.timestamp().try_into().ok()),
            if_unmodified_since: headers
                .get_one("If-Unmodified-Since")
                .and_then(|x| chrono::DateTime::parse_from_rfc2822(x).ok())
                .and_then(|x| x.timestamp().try_into().ok()),
        })
    }
}

#[derive(Debug)]
pub(crate) struct GetObjectOutput {
    data: BobData,
    content_type: ContentType,
}

impl Responder<'_, 'static> for GetObjectOutput {
    fn respond_to(self, _: &Request) -> response::Result<'static> {
        Response::build()
            .status(Status::Ok)
            .header(self.content_type)
            .header(Header::new(
                "Last-Modified",
                self.data.meta().timestamp().to_string(),
            ))
            .streamed_body(Cursor::new(self.data.into_inner()))
            .ok()
    }
}

#[get("/default/<key>")]
pub(crate) async fn get_object(
    bob: &State<BobServer>,
    key: Result<DataKey, StatusExt>,
    headers: GetObjectHeaders,
) -> Result<GetObjectOutput, StatusS3> {
    let key = key?.0;
    let opts = BobOptions::new_get(None);
    let data = bob.grinder().get(key, &opts).await?;
    let content_type = headers
        .content_type
        .unwrap_or_else(|| infer_data_type(&data));
    let last_modified = data.meta().timestamp();
    if let Some(time) = headers.if_modified_since {
        if time > last_modified {
            return Err(StatusS3::Status(Status::NotModified));
        }
    }
    if let Some(time) = headers.if_unmodified_since {
        if time < last_modified {
            return Err(StatusS3::Status(Status::PreconditionFailed));
        }
    }
    Ok(GetObjectOutput { data, content_type })
}

#[put("/default/<key>", data = "<data>", rank = 2)]
pub(crate) async fn put_object(
    bob: &State<BobServer>,
    key: Result<DataKey, StatusExt>,
    data: Data<'_>,
) -> Result<StatusS3, StatusS3> {
    let key = key?.0;
    let data_buf = data.open(ByteUnit::max_value()).into_bytes().await?.value;
    let data = BobData::new(
        data_buf,
        BobMeta::new(chrono::Local::now().timestamp() as u64),
    );

    let opts = BobOptions::new_put(None);
    bob.grinder().put(key, data, opts).await?;

    Ok(StatusS3::from(StatusExt::from(Status::Created)))
}

#[derive(Debug)]
pub(crate) struct CopyObjectHeaders {
    if_modified_since: Option<u64>,
    if_unmodified_since: Option<u64>,
    _source_key: DataKey,
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for CopyObjectHeaders {
    type Error = StatusS3;
    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        let headers = request.headers();
        let _source_key = match headers
            .get_one("x-amz-copy-source")
            .and_then(|x| x.parse().ok())
        {
            Some(key) => key,
            None => return Outcome::Forward(()),
        };
        Outcome::Success(CopyObjectHeaders {
            if_modified_since: headers
                .get_one("If-Modified-Since")
                .and_then(|x| chrono::DateTime::parse_from_rfc2822(x).ok())
                .and_then(|x| x.timestamp().try_into().ok()),
            if_unmodified_since: headers
                .get_one("If-Unmodified-Since")
                .and_then(|x| chrono::DateTime::parse_from_rfc2822(x).ok())
                .and_then(|x| x.timestamp().try_into().ok()),
            _source_key,
        })
    }
}

#[put("/default/<key>")]
pub(crate) async fn copy_object(
    bob: &State<BobServer>,
    key: Result<DataKey, StatusExt>,
    headers: CopyObjectHeaders,
) -> Result<StatusS3, StatusS3> {
    let key = key?.0;
    let opts = BobOptions::new_get(None);
    let data = bob.grinder().get(key, &opts).await?;
    let last_modified = data.meta().timestamp();
    if let Some(time) = headers.if_modified_since {
        if time > last_modified {
            return Err(StatusS3::Status(Status::NotModified));
        }
    }
    if let Some(time) = headers.if_unmodified_since {
        if time < last_modified {
            return Err(StatusS3::Status(Status::PreconditionFailed));
        }
    }
    let data = BobData::new(
        data.into_inner(),
        BobMeta::new(chrono::Local::now().timestamp() as u64),
    );

    let opts = BobOptions::new_put(None);
    bob.grinder().put(key, data, opts).await?;

    Ok(StatusS3::from(StatusExt::from(Status::Ok)))
}
