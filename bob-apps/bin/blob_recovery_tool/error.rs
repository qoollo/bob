use super::prelude::*;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("record header validation error: {0}")]
    RecordHeaderValidation(String),
    #[error("index header validation error: {0}")]
    IndexHeaderValidation(String),
    #[error("index validation error: {0}")]
    IndexValidation(String),
    #[error("blob header validation error: {0}")]
    BlobHeaderValidation(String),
    #[error("record validation error: {0}")]
    RecordValidation(String),
    #[error("skip data error: {0}")]
    SkipRecordData(String),
    #[error("unsupported migration from {0} to {1}")]
    UnsupportedMigration(u32, u32),
}

impl Error {
    pub(crate) fn record_header_validation_error(message: impl Into<String>) -> Self {
        Self::RecordHeaderValidation(message.into())
    }

    pub(crate) fn index_header_validation_error(message: impl Into<String>) -> Self {
        Self::IndexHeaderValidation(message.into())
    }

    pub(crate) fn index_validation_error(message: impl Into<String>) -> Self {
        Self::IndexValidation(message.into())
    }

    pub(crate) fn blob_header_validation_error(message: impl Into<String>) -> Self {
        Self::BlobHeaderValidation(message.into())
    }

    pub(crate) fn record_validation_error(message: impl Into<String>) -> Self {
        Self::RecordValidation(message.into())
    }

    pub(crate) fn skip_record_data_error(message: impl Into<String>) -> Self {
        Self::SkipRecordData(message.into())
    }

    pub(crate) fn unsupported_migration(from: u32, to: u32) -> Self {
        Self::UnsupportedMigration(from, to)
    }
}
