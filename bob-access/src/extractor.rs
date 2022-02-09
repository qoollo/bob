use crate::{credentials::Credentials, error::Error};
use http::Request;
use tonic::transport::server::TcpConnectInfo;

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

impl<T> Extractor<Request<T>> for BasicExtractor {
    fn extract(&self, req: &Request<T>) -> Result<Credentials, Error> {
        let meta = req.headers();
        let username = meta
            .get("username")
            .ok_or_else(|| Error::CredentialsNotProvided("username".to_string()))?
            .to_str()
            .map_err(Error::ConversionError)?;
        let password = meta
            .get("password")
            .ok_or_else(|| Error::CredentialsNotProvided("password".to_string()))?
            .to_str()
            .map_err(Error::ConversionError)?;
        let addr = req
            .extensions()
            .get::<TcpConnectInfo>()
            .and_then(|i| i.remote_addr());
        let creds = Credentials::builder()
            .with_username_password(username, password)
            .with_address(addr)
            .build();
        Ok(creds)
    }
}

#[derive(Debug, Clone, Default)]
pub struct TokenExtractor {}

impl<T> Extractor<Request<T>> for TokenExtractor {
    fn extract(&self, req: &Request<T>) -> Result<Credentials, Error> {
        let meta = req.headers();
        let token = meta
            .get("token")
            .ok_or_else(|| Error::CredentialsNotProvided("token".to_string()))?
            .to_str()
            .map_err(Error::ConversionError)?;
        let addr = req
            .extensions()
            .get::<TcpConnectInfo>()
            .and_then(|i| i.remote_addr());
        let creds = Credentials::builder()
            .with_token(token)
            .with_address(addr)
            .build();
        Ok(creds)
    }
}

#[derive(Debug, Clone, Default)]
pub struct MultiExtractor {
    basic_extractor: BasicExtractor,
    token_extractor: TokenExtractor,
}

impl<T> Extractor<Request<T>> for MultiExtractor {
    fn extract(&self, req: &Request<T>) -> Result<Credentials, Error> {
        let basic_credentials = self.basic_extractor.extract(req);
        let token_credentials = self.token_extractor.extract(req);
        match (basic_credentials.is_ok(), token_credentials.is_ok()) {
            (true, true) => Err(Error::MultipleCredentialsTypes),
            (true, false) => basic_credentials,
            (false, true) => token_credentials,
            _ => Ok(Credentials::builder()
                // .with_address(req.remote_addr())
                .build()),
        }
    }
}
