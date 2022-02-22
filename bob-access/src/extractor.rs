use std::net::SocketAddr;

use crate::{credentials::Credentials, error::Error};
use axum::extract::RequestParts;
use http::{Extensions, HeaderMap, Request};
use tonic::{transport::server::TcpConnectInfo, Request as TonicRequest};

pub trait Extractor {
    fn headers(&self) -> Option<&HeaderMap>;
    fn extensions(&self) -> Option<&Extensions>;
}

pub trait ExtractorExt {
    fn extract(&self) -> Result<Credentials, Error>;
    fn extract_basic(
        &self,
        _header_map: &HeaderMap,
        _addr: Option<SocketAddr>,
    ) -> Result<Option<Credentials>, Error> {
        Ok(None)
    }
    fn extract_token(
        &self,
        _header_map: &HeaderMap,
        _addr: Option<SocketAddr>,
    ) -> Result<Option<Credentials>, Error> {
        Ok(None)
    }
}

impl<T: Extractor> ExtractorExt for T {
    fn extract(&self) -> Result<Credentials, Error> {
        let header_map = if let Some(header_map) = self.headers() {
            header_map
        } else {
            return Ok(Credentials::default());
        };
        let addr = self
            .extensions()
            .and_then(|ext| ext.get::<TcpConnectInfo>()?.remote_addr());
        let basic_credentials = self.extract_basic(header_map, addr)?;
        let token_credentials = self.extract_token(header_map, addr)?;
        match (basic_credentials, token_credentials) {
            (Some(_), Some(_)) => Err(Error::MultipleCredentialsTypes),
            (Some(basic_credentials), None) => Ok(basic_credentials),
            (None, Some(token_credentials)) => Ok(token_credentials),
            _ => Ok(Credentials::builder()
                // .with_address(req.remote_addr())
                .build()),
        }
    }

    fn extract_basic(
        &self,
        header_map: &HeaderMap,
        addr: Option<SocketAddr>,
    ) -> Result<Option<Credentials>, Error> {
        let username = if let Some(username) = header_map.get("username") {
            username.to_str().map_err(Error::ConversionError)?
        } else {
            return Ok(None);
        };
        let password = if let Some(password) = header_map.get("password") {
            password.to_str().map_err(Error::ConversionError)?
        } else {
            return Ok(None);
        };
        let creds = Credentials::builder()
            .with_username_password(username, password)
            .with_address(addr)
            .build();
        Ok(Some(creds))
    }

    fn extract_token(
        &self,
        header_map: &HeaderMap,
        addr: Option<SocketAddr>,
    ) -> Result<Option<Credentials>, Error> {
        let token = if let Some(token) = header_map.get("token") {
            token.to_str().map_err(Error::ConversionError)?
        } else {
            return Ok(None);
        };
        let creds = Credentials::builder()
            .with_token(token)
            .with_address(addr)
            .build();
        Ok(Some(creds))
    }
}

impl<T> ExtractorExt for TonicRequest<T> {
    fn extract(&self) -> Result<Credentials, Error> {
        todo!()
    }
}

impl<B> Extractor for RequestParts<B> {
    fn headers(&self) -> Option<&HeaderMap> {
        self.headers()
    }

    fn extensions(&self) -> Option<&Extensions> {
        self.extensions()
    }
}

impl<T> Extractor for Request<T> {
    fn headers(&self) -> Option<&HeaderMap> {
        Some(self.headers())
    }

    fn extensions(&self) -> Option<&Extensions> {
        Some(self.extensions())
    }
}
