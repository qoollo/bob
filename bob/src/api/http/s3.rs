use std::io::{Cursor, Read};

use super::{data_to_type, StatusExt};
use crate::server::Server as BobServer;
use bob_common::data::{BobData, BobKey, BobMeta, BobOptions};
use rocket::{
    get,
    http::{Header, Status},
    put, response,
    response::Responder,
    Data, Request, Response, Route, State,
};

#[derive(Debug)]
pub(crate) struct GetObjectInput {}

#[derive(Debug)]
pub(crate) struct GetObjectOutput {
    data: BobData,
}

impl<'r> Responder<'r> for GetObjectOutput {
    fn respond_to(self, _: &Request) -> response::Result<'r> {
        Response::build()
            .status(Status::Ok)
            .header(data_to_type(&self.data))
            .header(Header::new(
                "Last-Modified",
                self.data.meta().timestamp().to_string(),
            ))
            .sized_body(Cursor::new(self.data.into_inner()))
            .ok()
    }
}

#[derive(Debug)]
pub(crate) struct StatusS3 {
    inner: StatusExt,
}

impl From<StatusExt> for StatusS3 {
    fn from(inner: StatusExt) -> Self {
        Self { inner }
    }
}

impl<'r> Responder<'r> for StatusS3 {
    fn respond_to(self, request: &Request) -> response::Result<'r> {
        let resp = self.inner.respond_to(request)?;
        Response::build().status(resp.status()).ok()
    }
}

pub fn routes() -> impl Into<Vec<Route>> {
    routes![get_object, put_object]
}

#[get("/default/<key>")]
pub(crate) fn get_object(bob: State<BobServer>, key: BobKey) -> Result<GetObjectOutput, StatusS3> {
    let opts = BobOptions::new_get(None);
    let result = super::runtime()
        .block_on(async { bob.grinder().get(key, &opts).await })
        .map_err(|err| -> StatusExt { err.into() })?;
    Ok(GetObjectOutput { data: result })
}

#[put("/default/<key>", data = "<data>")]
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
    super::runtime()
        .block_on(async { bob.grinder().put(key, data, opts).await })
        .map_err(|err| -> StatusExt { err.into() })?;

    Ok(StatusS3::from(StatusExt::from(Status::Created)))
}
