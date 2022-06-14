use crate::{credentials::{Credentials, CredentialsBuilder}, error::Error};
use axum::extract::RequestParts;
use http::{Extensions, HeaderMap, Request};
use tonic::{transport::server::TcpConnectInfo};
use super::{credentials_type, CredentialsType};

pub trait Extractor {
    fn headers(&self) -> Option<&HeaderMap>;
    fn extensions(&self) -> Option<&Extensions>;
}

pub trait ExtractorExt {
    fn extract(&self) -> Result<Credentials, Error>;
    fn extract_basic(
        &self,
        _header_map: &HeaderMap,
        _builder: CredentialsBuilder,
    ) -> Result<Credentials, Error>;

    fn extract_token(
        &self,
        _header_map: &HeaderMap,
        _builder: CredentialsBuilder,
    ) -> Result<Credentials, Error>;
}

fn prepare_builder<T: Extractor>(slf: &T) -> Result<(&HeaderMap, CredentialsBuilder), Error> {
    let header_map = if let Some(header_map) = slf.headers() {
        header_map
    } else {
        return Err(Error::CredentialsNotProvided("can't extract headers".into()));
    };
    let addr = slf
        .extensions()
        .and_then(|ext| ext.get::<TcpConnectInfo>()?.remote_addr());
    let mut builder = Credentials::builder();
    builder.with_address(addr);
    Ok((header_map, builder))
}

impl<T: Extractor> ExtractorExt for T {
    fn extract(&self) -> Result<Credentials, Error> {
        let cred_tp = credentials_type();
        match cred_tp {
            CredentialsType::Stub => {
                return Ok(Credentials::default());
            },
            CredentialsType::Basic => {
                let (header_map, builder) = prepare_builder(self)?;
                return Ok(self.extract_basic(header_map, builder)?);
            },
            CredentialsType::Token => {
                let (header_map, builder) = prepare_builder(self)?;
                return Ok(self.extract_token(header_map, builder)?);
            },
        }
    }

    fn extract_basic(
        &self,
        header_map: &HeaderMap,
        mut builder: CredentialsBuilder,
    ) -> Result<Credentials, Error> {
        if let (Some(username), Some(password)) = 
            (parse_header_field(header_map, "username")?, 
            parse_header_field(header_map, "password")?)
        {
            let creds = builder
                .with_username_password(username, password)
                .build();
            Ok(creds)
        } else {
            Err(Error::CredentialsNotProvided("missing username or password".into()))
        }
    }

    fn extract_token(
        &self,
        header_map: &HeaderMap,
        mut builder: CredentialsBuilder,
    ) -> Result<Credentials, Error> {
        if let Some(token) = parse_header_field(header_map, "token")? {
            let creds = builder
                .with_token(token)
                .build();
            Ok(creds)
        } else {
            Err(Error::CredentialsNotProvided("missing token".into()))
        }
    }
}

fn parse_header_field<'a>(header: &'a HeaderMap, field: &str) -> Result<Option<&'a str>, Error> {
    if let Some(value) = header.get(field) {
        value.to_str().map_err(Error::ConversionError).map(|r| Some(r))
    } else {
        Ok(None)
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
