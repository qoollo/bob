use crate::server::Server as BobServer;
use bob_common::data::{BobKey, BobOptions};
use rocket::{get, http::ContentType, http::Status, put, response::Content, Data, Route, State};

use super::StatusExt;

pub fn routes() -> impl Into<Vec<Route>> {
    routes![get_object]
}

#[get("/<_bucket>/<key>")]
pub(crate) fn get_object(
    bob: State<BobServer>,
    _bucket: String,
    key: BobKey,
) -> Result<Content<Vec<u8>>, StatusExt> {
    let opts = BobOptions::new_get(None);
    let result = super::runtime()
        .block_on(async { bob.grinder().get(key, &opts).await })
        .map_err(|_err| -> StatusExt {
            StatusExt::new(Status::InternalServerError, false, "Error".into())
        })?;
    Ok(Content(ContentType::Any, result.inner().to_owned()))
}

#[put("/<_bucket>/<_key>", data = "<_data>")]
pub fn put_object(_bob: State<BobServer>, _bucket: String, _key: i64, _data: Data) -> String {
    "String".into()
}
