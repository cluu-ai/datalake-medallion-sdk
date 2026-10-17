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
}
