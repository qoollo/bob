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
            .ok_or_else(|| Error::credentials_not_provided("username"))?
            .to_str()
            .map_err(Error::conversion_error)?;
        let password = meta
            .get("password")
            .ok_or_else(|| Error::credentials_not_provided("password"))?
            .to_str()
            .map_err(Error::conversion_error)?;
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

// impl<'r> Extractor<RRequest<'r>> for BasicExtractor {
//     fn extract(&self, req: &RRequest) -> Result<Credentials, Error> {
//         let headers = req.headers();
//         let username = headers
//             .get_one("username")
//             .ok_or_else(|| Error::credentials_not_provided("username"))?;
//         let password = headers
//             .get_one("password")
//             .ok_or_else(|| Error::credentials_not_provided("password"))?;
//         Ok(Credentials::builder()
//             .with_username_password(username, password)
//             .with_address(req.remote())
//             .build())
//     }
// }

#[derive(Debug, Clone, Default)]
pub struct TokenExtractor {}

impl<T> Extractor<Request<T>> for TokenExtractor {
    fn extract(&self, req: &Request<T>) -> Result<Credentials, Error> {
        let meta = req.headers();
        let token = meta
            .get("token")
            .ok_or_else(|| Error::credentials_not_provided("token"))?
            .to_str()
            .map_err(Error::conversion_error)?;
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

// impl<'r> Extractor<RRequest<'r>> for TokenExtractor {
//     fn extract(&self, req: &RRequest) -> Result<Credentials, Error> {
//         let headers = req.headers();
//         let token = headers
//             .get_one("token")
//             .ok_or_else(|| Error::credentials_not_provided("token"))?;
//         Ok(Credentials::builder()
//             .with_token(token)
//             .with_address(req.remote())
//             .build())
//     }
// }

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
            (true, true) => Err(Error::multiple_credentials_types()),
            (true, false) => basic_credentials,
            (false, true) => token_credentials,
            _ => Ok(Credentials::builder()
                // .with_address(req.remote_addr())
                .build()),
        }
    }
}

// impl<'r> Extractor<RRequest<'r>> for MultiExtractor {
//     fn extract(&self, req: &RRequest) -> Result<Credentials, Error> {
//         let basic_credentials = self.basic_extractor.extract(req);
//         let token_credentials = self.token_extractor.extract(req);
//         match (basic_credentials.is_ok(), token_credentials.is_ok()) {
//             (true, true) => Err(Error::multiple_credentials_types()),
//             (true, false) => basic_credentials,
//             (false, true) => token_credentials,
//             _ => Ok(Credentials::builder().with_address(req.remote()).build()),
//         }
//     }
// }
