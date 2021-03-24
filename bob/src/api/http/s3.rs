use super::StatusExt;
use crate::server::Server as BobServer;
use bob_common::data::{BobKey, BobOptions};
use rocket::{
    get,
    http::{ContentType, Status},
    put,
    response::Content,
    Data, Route, State,
};

pub fn routes() -> impl Into<Vec<Route>> {
    routes![get_object, put_object]
}

#[get("/default/<key>")]
pub(crate) fn get_object(
    bob: State<BobServer>,
    key: BobKey,
) -> Result<Content<Vec<u8>>, StatusExt> {
    let opts = BobOptions::new_get(None);
    let result = super::runtime()
        .block_on(async { bob.grinder().get(key, &opts).await })
        .map_err(|err| -> StatusExt { err.into() })?;
    Ok(Content(ContentType::Any, result.inner().to_owned()))
}

#[put("/default/<key>", data = "<data>")]
pub fn put_object(bob: State<BobServer>, key: BobKey, data: Data) -> Status {
    Status::NotImplemented
}
