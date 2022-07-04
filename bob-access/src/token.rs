use jsonwebtoken::{DecodingKey, Validation};
use serde::{Deserialize, Serialize};

use crate::error::Error;

#[derive(Debug)]
pub struct TokenDecoder {
    _key: DecodingKey<'static>,
    _validation: Validation,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    username: String,
    password: String,
}

impl TokenDecoder {
    pub fn _new(secret: impl AsRef<[u8]>) -> Self {
        Self {
            _key: DecodingKey::from_secret(secret.as_ref()).into_static(),
            _validation: Validation {
                validate_exp: false,
                ..Default::default()
            },
        }
    }

    pub fn _decode_token(&self, token: impl AsRef<str>) -> Result<Claims, Error> {
        jsonwebtoken::decode(token.as_ref(), &self._key, &self._validation)
            .map(|data| data.claims)
            .map_err(|e| Error::InvalidToken(e.to_string()))
    }
}
