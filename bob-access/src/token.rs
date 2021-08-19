use jsonwebtoken::{DecodingKey, Validation};
use serde::{Deserialize, Serialize};

use crate::error::Error;

#[derive(Debug)]
pub struct TokenDecoder {
    key: DecodingKey<'static>,
    validation: Validation,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    username: String,
    password: String,
}

impl TokenDecoder {
    pub fn _new(secret: impl AsRef<[u8]>) -> Self {
        Self {
            key: DecodingKey::from_secret(secret.as_ref()).into_static(),
            validation: Validation {
                validate_exp: false,
                ..Default::default()
            },
        }
    }

    pub fn _decode_token(&self, token: impl AsRef<str>) -> Result<Claims, Error> {
        jsonwebtoken::decode(token.as_ref(), &self.key, &self.validation)
            .map(|data| data.claims)
            .map_err(|e| Error::invalid_token(e.to_string()))
    }
}
