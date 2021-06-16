use crate::{credentials::Credentials, error::Error};
use rocket::Request as RRequest;
use tonic::Request as TRequest;

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
    fn extract(&self, _req: &Request) -> Result<Credentials, Error> {
        Ok(Credentials::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct BasicExtractor {}

impl<T> Extractor<TRequest<T>> for BasicExtractor {
    fn extract(&self, req: &TRequest<T>) -> Result<Credentials, Error> {
        let meta = req.metadata();
        let username = meta
            .get("username")
            .ok_or_else(|| Error::credentials_not_provided("username"))?
            .to_str()
            .map_err(|e| Error::unknown(e.to_string()))?;
        let password = meta
            .get("password")
            .ok_or_else(|| Error::credentials_not_provided("password"))?
            .to_str()
            .map_err(|e| Error::unknown(e.to_string()))?;

        Ok(Credentials::new()
            .with_username_password(username, password)
            .with_address(req.remote_addr()))
    }
}

impl<'r> Extractor<RRequest<'r>> for BasicExtractor {
    fn extract(&self, req: &RRequest) -> Result<Credentials, Error> {
        let headers = req.headers();
        let username = headers
            .get_one("username")
            .ok_or_else(|| Error::credentials_not_provided("username"))?;
        let password = headers
            .get_one("password")
            .ok_or_else(|| Error::credentials_not_provided("password"))?;
        Ok(Credentials::new()
            .with_username_password(username, password)
            .with_address(req.remote()))
    }
}

#[derive(Debug, Clone, Default)]
pub struct TokenExtractor {}

impl<T> Extractor<TRequest<T>> for TokenExtractor {
    fn extract(&self, req: &TRequest<T>) -> Result<Credentials, Error> {
        let meta = req.metadata();
        let token = meta
            .get("token")
            .ok_or_else(|| Error::credentials_not_provided("token"))?
            .to_str()
            .map_err(|e| Error::unknown(e.to_string()))?;
        Ok(Credentials::new()
            .with_token(token)
            .with_address(req.remote_addr()))
    }
}

impl<'r> Extractor<RRequest<'r>> for TokenExtractor {
    fn extract(&self, req: &RRequest) -> Result<Credentials, Error> {
        let headers = req.headers();
        let token = headers
            .get_one("token")
            .ok_or_else(|| Error::credentials_not_provided("token"))?;
        Ok(Credentials::new()
            .with_token(token)
            .with_address(req.remote()))
    }
}

#[derive(Debug, Clone, Default)]
pub struct MultiExtractor {
    basic_extractor: BasicExtractor,
    token_extractor: TokenExtractor,
}

impl<T> Extractor<TRequest<T>> for MultiExtractor {
    fn extract(&self, req: &TRequest<T>) -> Result<Credentials, Error> {
        let basic_credentials = self.basic_extractor.extract(req);
        let token_credentials = self.token_extractor.extract(req);
        if basic_credentials.is_ok() && token_credentials.is_ok() {
            Err(Error::unknown("multiple credentials types provided"))
        } else if basic_credentials.is_ok() {
            basic_credentials
        } else if token_credentials.is_ok() {
            token_credentials
        } else {
            Ok(Credentials::new().with_address(req.remote_addr()))
        }
    }
}

impl<'r> Extractor<RRequest<'r>> for MultiExtractor {
    fn extract(&self, req: &RRequest) -> Result<Credentials, Error> {
        let basic_credentials = self.basic_extractor.extract(req);
        let token_credentials = self.token_extractor.extract(req);
        if basic_credentials.is_ok() && token_credentials.is_ok() {
            Err(Error::unknown("multiple credentials types provided"))
        } else if basic_credentials.is_ok() {
            basic_credentials
        } else if token_credentials.is_ok() {
            token_credentials
        } else {
            Ok(Credentials::new().with_address(req.remote()))
        }
    }
}
