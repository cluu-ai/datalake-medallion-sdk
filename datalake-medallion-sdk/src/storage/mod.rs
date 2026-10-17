//! Object storage port, adapters and Parquet encoding.

pub mod memory;
pub mod parquet;
pub mod s3;

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::DatalakeError;
use crate::tenant::ObjectPath;

pub use memory::InMemoryStorage;
pub use parquet::ParquetEncoder;
pub use s3::{S3Options, S3Storage, StaticCredentials};

/// Media type of Bronze data objects.
pub const PARQUET_CONTENT_TYPE: &str = "application/vnd.apache.parquet";
/// Media type of dead-letter objects.
pub const NDJSON_CONTENT_TYPE: &str = "application/x-ndjson";

/// Port for writing immutable objects. Implement it to target another store.
/// The bucket comes with each path, so one adapter serves every tenant.
#[async_trait]
pub trait ObjectStorage: Send + Sync {
    /// Writes `body` at `path`.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Storage`] when the write fails.
    async fn put(
        &self,
        path: &ObjectPath,
        body: Bytes,
        content_type: &str,
    ) -> Result<(), DatalakeError>;
}
