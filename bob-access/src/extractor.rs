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

fn username_password_from_credentials(credentials: &str) -> Result<(String, String), Error> {
    let credentials = base64::decode(credentials)
        .map_err(|e| Error::ConversionError(format!("bad base64 credentials: {}", e)))?;
    let str_cred = String::from_utf8(credentials)
        .map_err(|e| Error::ConversionError(format!("invalid utf8 credentials characters: {}", e)))?;
    let mut user_pass = str_cred.split_terminator(":");
    match (user_pass.next(), user_pass.next()) {
        // (user & pass), (empty user & pass) cases
        (Some(username), Some(password)) => {
            Ok((username.into(), password.into()))
        },
        // (user & empty pass), (empty user & empty pass) cases
        (Some(username), None) => {
            Ok((username.into(), "".into()))
        },
        _ => Err(Error::CredentialsNotProvided("missing username or password".into()))
    }
}

fn nodename_from_credentials(credentials: &str) -> Result<String, Error> {
    let credentials = base64::decode(credentials)
        .map_err(|e| Error::ConversionError(format!("bad base64 credentials: {}", e)))?;
    let nodename = String::from_utf8(credentials)
        .map_err(|e| Error::ConversionError(format!("invalid utf8 credentials characters: {}", e)))?;
    Ok(nodename)
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
        let auth_header = self.get_header("authorization")?;
        if let Some(auth_header) = auth_header {
            let mut parts = auth_header.split_whitespace();
            match (parts.next(), parts.next()) {
                (Some("Basic"), Some(credentials)) => {
                    let (username, password) = username_password_from_credentials(credentials)?;
                    let creds = builder
                        .with_username_password(username, password)
                        .build();
                    Ok(creds)
                },
                (Some("InterNode"), Some(credentials)) => {
                    let node_name = nodename_from_credentials(credentials)?;
                    let creds = builder
                        .with_nodename(node_name)
                        .build();
                    Ok(creds)
                },
                _ => Err(Error::CredentialsNotProvided("bad authorization header".into())),
            }
        } else {
            // Fallback to special "default" user, when no credentials were provided
            let creds = builder
                .with_username_password("default", "")
                .build();
            Ok(creds)
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
