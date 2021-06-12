use crate::{credentials::Credentials, error::Error};

pub trait Extractor<Request>: Clone {
    fn extract(&self, req: &Request) -> Result<Credentials, Error>;
}

#[derive(Debug, Default, Clone)]
pub struct StubExtractor {}

impl StubExtractor {
    pub fn new() -> Self {
        Self {}
    }
}

impl<Request> Extractor<Request> for StubExtractor {
    fn extract(&self, req: &Request) -> Result<Credentials, Error> {
        Ok(Credentials::default())
    }
}
