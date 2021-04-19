use std::{
    convert::TryInto,
    io::{Cursor, Read},
    str::FromStr,
};

use super::{infer_data_type, StatusExt};
use crate::server::Server as BobServer;
use bob_common::data::{BobData, BobKey, BobMeta, BobOptions};
use rocket::{
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

impl<'r> Responder<'r> for StatusS3 {
    fn respond_to(self, request: &Request) -> response::Result<'r> {
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
    routes![get_object, put_object]
}

#[derive(Debug, Default)]
pub(crate) struct GetObjectHeaders {
    content_type: Option<ContentType>,
    if_modified_since: Option<u64>,
    if_unmodified_since: Option<u64>,
}

impl<'r> FromRequest<'_, 'r> for GetObjectHeaders {
    type Error = StatusS3;
    fn from_request(request: &Request<'r>) -> Outcome<Self, Self::Error> {
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

impl<'r> Responder<'r> for GetObjectOutput {
    fn respond_to(self, _: &Request) -> response::Result<'r> {
        Response::build()
            .status(Status::Ok)
            .header(self.content_type)
            .header(Header::new(
                "Last-Modified",
                self.data.meta().timestamp().to_string(),
            ))
            .sized_body(Cursor::new(self.data.into_inner()))
            .ok()
    }
}

#[get("/default/<key>")]
pub(crate) fn get_object(
    bob: State<BobServer>,
    key: BobKey,
    headers: GetObjectHeaders,
) -> Result<GetObjectOutput, StatusS3> {
    let opts = BobOptions::new_get(None);
    let data = bob
        .block_on(async { bob.grinder().get(key, &opts).await })
        .map_err(|err| -> StatusExt { err.into() })?;
    let content_type = match headers.content_type {
        Some(t) => t,
        None => infer_data_type(&data),
    };
    let last_modified = data.meta().timestamp();
    match headers.if_modified_since {
        Some(time) if time > last_modified => return Err(StatusS3::Status(Status::NotModified)),
        _ => {}
    };
    match headers.if_unmodified_since {
        Some(time) if time < last_modified => {
            return Err(StatusS3::Status(Status::PreconditionFailed))
        }
        _ => {}
    };
    Ok(GetObjectOutput { data, content_type })
}

#[put("/default/<key>", data = "<data>", rank = 2)]
pub(crate) fn put_object(
    bob: State<BobServer>,
    key: BobKey,
    data: Data,
) -> Result<StatusS3, StatusS3> {
    let mut data_buf = vec![];
    data.open()
        .read_to_end(&mut data_buf)
        .map_err(|err| -> StatusExt { err.into() })?;
    let data = BobData::new(
        data_buf,
        BobMeta::new(chrono::Local::now().timestamp() as u64),
    );

    let opts = BobOptions::new_put(None);
    bob.block_on(async { bob.grinder().put(key, data, opts).await })
        .map_err(|err| -> StatusExt { err.into() })?;

    Ok(StatusS3::from(StatusExt::from(Status::Created)))
}

#[derive(Debug, Default)]
pub(crate) struct CopyObjectHeaders {
    if_modified_since: Option<u64>,
    if_unmodified_since: Option<u64>,
    source_key: BobKey,
}

impl<'r> FromRequest<'_, 'r> for CopyObjectHeaders {
    type Error = StatusS3;
    fn from_request(request: &Request<'r>) -> Outcome<Self, Self::Error> {
        let headers = request.headers();
        let source_key = match headers
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
            source_key,
        })
    }
}

#[put("/default/<key>")]
pub(crate) fn copy_object(
    bob: State<BobServer>,
    key: BobKey,
    headers: CopyObjectHeaders,
) -> Result<StatusS3, StatusS3> {
    let opts = BobOptions::new_get(None);
    let data = bob
        .block_on(async { bob.grinder().get(key, &opts).await })
        .map_err(|err| -> StatusExt { err.into() })?;
    let last_modified = data.meta().timestamp();
    match headers.if_modified_since {
        Some(time) if time > last_modified => return Err(StatusS3::Status(Status::NotModified)),
        _ => {}
    };
    match headers.if_unmodified_since {
        Some(time) if time < last_modified => {
            return Err(StatusS3::Status(Status::PreconditionFailed))
        }
        _ => {}
    };
    let data = BobData::new(
        data.into_inner(),
        BobMeta::new(chrono::Local::now().timestamp() as u64),
    );

    let opts = BobOptions::new_put(None);
    bob.block_on(async { bob.grinder().put(key, data, opts).await })
        .map_err(|err| -> StatusExt { err.into() })?;

    Ok(StatusS3::from(StatusExt::from(Status::Ok)))
}
