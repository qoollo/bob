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

        Ok(Credentials::basic(username, password))
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
        Ok(Credentials::basic(username, password))
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
        Ok(Credentials::token(token))
    }
}

impl<'r> Extractor<RRequest<'r>> for TokenExtractor {
    fn extract(&self, req: &RRequest) -> Result<Credentials, Error> {
        let headers = req.headers();
        let token = headers
            .get_one("token")
            .ok_or_else(|| Error::credentials_not_provided("token"))?;
        Ok(Credentials::token(token))
    }
}

#[derive(Debug, Clone, Default)]
pub struct AddressExtractor {}

impl<T> Extractor<TRequest<T>> for AddressExtractor {
    fn extract(&self, req: &TRequest<T>) -> Result<Credentials, Error> {
        req.remote_addr()
            .map(|addr| Credentials::address(addr))
            .ok_or_else(|| Error::credentials_not_provided("address"))
    }
}

impl<'r> Extractor<RRequest<'r>> for AddressExtractor {
    fn extract(&self, req: &RRequest) -> Result<Credentials, Error> {
        // X-Real-IP header not allowed because of possible security issues
        req.remote()
            .map(|addr| Credentials::address(addr))
            .ok_or_else(|| Error::credentials_not_provided("address"))
    }
}

#[derive(Debug, Clone, Default)]
pub struct MultiExtractor {
    basic_extractor: BasicExtractor,
    token_extractor: TokenExtractor,
    address_extractor: AddressExtractor,
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
            self.address_extractor.extract(req)
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
            self.address_extractor.extract(req)
        }
    }
}
