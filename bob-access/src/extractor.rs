use crate::{credentials::{Credentials, CredentialsBuilder}, error::Error, AuthenticationType};
use axum::extract::RequestParts;
use http::Request;
use tonic::{transport::server::TcpConnectInfo, Request as TonicRequest};

pub trait Extractor {
    fn get_header(&self, header: &str) -> Result<Option<&str>, Error>;
    fn get_extension<T: Send + Sync + 'static>(&self) -> Option<&T>;
}

pub trait ExtractorExt {
    fn extract(&self, cred_type: AuthenticationType) -> Result<Credentials, Error>;
    fn extract_basic(&self) -> Result<Credentials, Error>;
    fn extract_token(&self) -> Result<Credentials, Error>;
}

fn prepare_builder<T: Extractor>(slf: &T) -> Result<CredentialsBuilder, Error> {
    let addr = slf
        .get_extension::<TcpConnectInfo>()
        .and_then(|ext| ext.remote_addr());
    let mut builder = Credentials::builder();
    builder.with_address(addr);
    Ok(builder)
}

impl<T: Extractor> ExtractorExt for T {
    fn extract(&self, cred_type: AuthenticationType) -> Result<Credentials, Error> {
        match cred_type {
            AuthenticationType::None => {
                return Ok(Credentials::default());
            },
            AuthenticationType::Basic => {
                return self.extract_basic();
            },
            AuthenticationType::Token => {
                return self.extract_token();
            },
        }
    }

    fn extract_basic(&self) -> Result<Credentials, Error> {
        let mut builder = prepare_builder(self)?;
        if let (Some(username), Some(password)) = 
            (self.get_header("username")?,
            self.get_header("password")?)
        {
            let creds = builder
                .with_username_password(username, password)
                .build();
            Ok(creds)
        } else {
            if let Some(node_name) = self.get_header("node_name")? {
                let creds = builder
                    .with_nodename(node_name)
                    .build();
                Ok(creds)
            } else {
                Err(Error::CredentialsNotProvided("missing username or password".into()))
            }
        }
    }

    fn extract_token(&self) -> Result<Credentials, Error> {
        let mut builder = prepare_builder(self)?;
        if let Some(token) = self.get_header("token")? {
            let creds = builder
                .with_token(token)
                .build();
            Ok(creds)
        } else {
            if let Some(node_name) = self.get_header("node_name")? {
                let creds = builder
                    .with_nodename(node_name)
                    .build();
                Ok(creds)
            } else {
                Err(Error::CredentialsNotProvided("missing token".into()))
            }
        }
    }
}

impl<B> Extractor for RequestParts<B> {
    fn get_header(&self, header: &str) -> Result<Option<&str>, Error> {
        if let Some(Some(v)) = self.headers().map(|hs| hs.get(header)) {
            v.to_str().map_err(|e| Error::ConversionError(e.to_string())).map(|r| Some(r))
        } else {
            Ok(None)
        }
    }

    fn get_extension<M: Send + Sync + 'static>(&self) -> Option<&M> {
        self.extensions().and_then(|ext| ext.get::<M>())
    }
}

impl<T> Extractor for Request<T> {
    fn get_header(&self, header: &str) -> Result<Option<&str>, Error> {
        if let Some(v) = self.headers().get(header) {
            v.to_str().map_err(|e| Error::ConversionError(e.to_string())).map(|r| Some(r))
        } else {
            Ok(None)
        }
    }

    fn get_extension<M: Send + Sync + 'static>(&self) -> Option<&M> {
        self.extensions().get::<M>()
    }
}

impl<T> Extractor for TonicRequest<T> {
    fn get_header(&self, header: &str) -> Result<Option<&str>, Error> {
        if let Some(v) = self.metadata().get(header) {
            v.to_str().map_err(|e| Error::ConversionError(e.to_string())).map(|r| Some(r))
        } else {
            Ok(None)
        }
    }

    fn get_extension<M: Send + Sync + 'static>(&self) -> Option<&M> {
        self.extensions().get::<M>()
    }
}
