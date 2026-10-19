//! In-memory [`ObjectStorage`] for tests and local experiments.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use bytes::Bytes;

use super::ObjectStorage;
use crate::error::DatalakeError;
use crate::tenant::ObjectPath;

/// A stored object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredObject {
    /// Object body.
    pub body: Bytes,
    /// Media type given on write.
    pub content_type: String,
}

/// Thread-safe map of objects keyed by `bucket/key`. Clones share the same
/// contents.
#[derive(Debug, Clone, Default)]
pub struct InMemoryStorage {
    objects: Arc<Mutex<BTreeMap<String, StoredObject>>>,
    uploads: Arc<Mutex<Uploads>>,
}

/// Content type and parts (by number) of an unfinished upload.
type OpenUpload = (String, BTreeMap<i32, Bytes>);

/// Unfinished multipart uploads by id.
#[derive(Debug, Default)]
struct Uploads {
    next_id: u64,
    open: BTreeMap<String, OpenUpload>,
}

impl InMemoryStorage {
    /// Creates an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<String, StoredObject>> {
        self.objects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn uploads(&self) -> MutexGuard<'_, Uploads> {
        self.uploads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Multipart uploads started and neither completed nor aborted.
    #[must_use]
    pub fn open_uploads(&self) -> usize {
        self.uploads().open.len()
    }

    /// Copy of the object at `bucket/key` (an `s3://` URI without the scheme).
    #[must_use]
    pub fn get(&self, key: &str) -> Option<StoredObject> {
        self.lock().get(key).cloned()
    }

    /// Sorted `bucket/key` names.
    #[must_use]
    pub fn keys(&self) -> Vec<String> {
        self.lock().keys().cloned().collect()
    }

    /// Number of objects.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether the store is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }
}

#[async_trait]
impl ObjectStorage for InMemoryStorage {
    async fn put(
        &self,
        path: &ObjectPath,
        body: Bytes,
        content_type: &str,
    ) -> Result<(), DatalakeError> {
        self.lock().insert(
            format!("{}/{}", path.bucket, path.key),
            StoredObject {
                body,
                content_type: content_type.to_owned(),
            },
        );
        Ok(())
    }

    async fn create_multipart(
        &self,
        path: &ObjectPath,
        content_type: &str,
    ) -> Result<String, DatalakeError> {
        let mut uploads = self.uploads();
        uploads.next_id += 1;
        let id = format!("{}#{}", path.uri, uploads.next_id);
        uploads
            .open
            .insert(id.clone(), (content_type.to_owned(), BTreeMap::new()));
        Ok(id)
    }

    async fn upload_part(
        &self,
        _path: &ObjectPath,
        upload_id: &str,
        part_number: i32,
        body: Bytes,
    ) -> Result<String, DatalakeError> {
        let mut uploads = self.uploads();
        let (_, parts) = uploads
            .open
            .get_mut(upload_id)
            .ok_or_else(|| DatalakeError::storage("memory", "unknown upload id"))?;
        parts.insert(part_number, body);
        Ok(format!("etag-{part_number}"))
    }

    async fn complete_multipart(
        &self,
        path: &ObjectPath,
        upload_id: &str,
        parts: Vec<(i32, String)>,
    ) -> Result<(), DatalakeError> {
        let (content_type, stored) = self
            .uploads()
            .open
            .remove(upload_id)
            .ok_or_else(|| DatalakeError::storage("memory", "unknown upload id"))?;
        let mut body = Vec::new();
        for (number, _) in &parts {
            let part = stored
                .get(number)
                .ok_or_else(|| DatalakeError::storage("memory", "missing part"))?;
            body.extend_from_slice(part);
        }
        self.put(path, Bytes::from(body), &content_type).await
    }

    async fn abort_multipart(
        &self,
        _path: &ObjectPath,
        upload_id: &str,
    ) -> Result<(), DatalakeError> {
        self.uploads().open.remove(upload_id);
        Ok(())
    }
}
