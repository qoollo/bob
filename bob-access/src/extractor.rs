use std::net::SocketAddr;

use crate::{credentials::Credentials, error::Error};
use axum::extract::RequestParts;
use http::HeaderMap;
use tonic::transport::server::TcpConnectInfo;

pub trait Extractor {
    fn extract(&self) -> Result<Credentials, Error>;
    fn extract_basic(
        &self,
        header_map: &HeaderMap,
        addr: Option<SocketAddr>,
    ) -> Result<Option<Credentials>, Error>;
    fn extract_token(
        &self,
        header_map: &HeaderMap,
        addr: Option<SocketAddr>,
    ) -> Result<Option<Credentials>, Error>;
}

impl<B> Extractor for RequestParts<B> {
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
