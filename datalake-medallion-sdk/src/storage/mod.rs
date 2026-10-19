//! Object storage port, adapters and Parquet encoding.

pub mod memory;
pub mod parquet;
pub mod s3;

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::DatalakeError;
use crate::tenant::ObjectPath;

pub use memory::InMemoryStorage;

/// Content type of commit manifest entries.
pub const JSON_CONTENT_TYPE: &str = "application/json";
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

    /// Starts a multipart upload of `path` and returns its upload id. The
    /// object only becomes visible when
    /// [`complete_multipart`](Self::complete_multipart) succeeds. The default
    /// refuses; streams then write each object with one [`put`](Self::put).
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Storage`] when the store cannot start the upload.
    async fn create_multipart(
        &self,
        path: &ObjectPath,
        content_type: &str,
    ) -> Result<String, DatalakeError> {
        let _ = (path, content_type);
        Err(DatalakeError::storage(
            "multipart",
            "this store does not support multipart uploads",
        ))
    }

    /// Uploads part `part_number` (from 1) and returns its entity tag. On S3
    /// every part but the last must be at least 5 MiB.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Storage`] when the upload fails.
    async fn upload_part(
        &self,
        path: &ObjectPath,
        upload_id: &str,
        part_number: i32,
        body: Bytes,
    ) -> Result<String, DatalakeError> {
        let _ = (path, upload_id, part_number, body);
        Err(DatalakeError::storage("multipart", "not supported"))
    }

    /// Assembles the uploaded parts, in part-number order, into the object.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Storage`] when completion fails.
    async fn complete_multipart(
        &self,
        path: &ObjectPath,
        upload_id: &str,
        parts: Vec<(i32, String)>,
    ) -> Result<(), DatalakeError> {
        let _ = (path, upload_id, parts);
        Err(DatalakeError::storage("multipart", "not supported"))
    }

    /// Discards an unfinished upload and its parts.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Storage`] when the store refuses.
    async fn abort_multipart(
        &self,
        path: &ObjectPath,
        upload_id: &str,
    ) -> Result<(), DatalakeError> {
        let _ = (path, upload_id);
        Ok(())
    }
}
