//! Bronze commit manifests (opt-in, [`PipelineConfig::manifests`]).
//!
//! After a batch is committed (its quarantine and data objects uploaded),
//! one immutable JSON entry describes it under
//! `<bronze>/_manifests/tenant_id=<t>/data_type=<d>/date=<YYYY-MM-DD>/<commit_id>-<batch_id>.json`.
//! Consumers (a Silver job, a search indexer) read the entries in commit
//! order from a watermark instead of listing the data objects, and see only
//! complete batches: an entry is written after everything it lists.
//!
//! `commit_id` is a ULID taken when the batch is prepared, so a retried
//! upload rewrites the same entry. Ingesting the same `batch_id` again (a new
//! call) adds a new entry listing the same object keys: treat `batch_id` (or
//! the object URI) as the unit to replace, never append blindly.
//!
//! [`PipelineConfig::manifests`]: crate::pipeline::PipelineConfig::manifests

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, TimeUnit, TimestampMicrosecondType};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use crate::contracts::Contract;

/// Format version of the entries.
pub const MANIFEST_VERSION: u32 = 1;

/// An object a committed batch wrote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestObject {
    /// `s3://` URI.
    pub uri: String,
    /// Size in bytes.
    pub bytes: u64,
}

/// Earliest and latest event time of the rows a batch wrote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventRange {
    /// The contract's event-time field.
    pub field: String,
    /// RFC 3339, microseconds, UTC.
    pub min: String,
    /// RFC 3339, microseconds, UTC.
    pub max: String,
}

/// One committed batch (or one flushed part of a stream).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// [`MANIFEST_VERSION`].
    pub version: u32,
    /// ULID of the commit; entries sort by it.
    pub commit_id: String,
    /// Batch id, or stream id for streams.
    pub batch_id: String,
    /// Tenant the batch was written for.
    pub tenant_id: String,
    /// Bronze table (contract data type).
    pub data_type: String,
    /// Contract key (`domain/datatype/version`).
    pub contract: String,
    /// SHA-256 of the contract schema.
    pub schema_hash: String,
    /// When the entry was created (the time of `commit_id`).
    pub committed_at: String,
    /// Records submitted.
    pub records_received: usize,
    /// Records written to Bronze.
    pub records_ok: usize,
    /// Records quarantined.
    pub records_quarantined: usize,
    /// Parquet objects.
    pub data: Vec<ManifestObject>,
    /// Dead-letter (quarantine) objects.
    pub dead_letters: Vec<ManifestObject>,
    /// Event-time range of the rows, when the contract has a typed event time.
    pub event_time: Option<EventRange>,
    /// SDK version that wrote the batch.
    pub sdk_version: String,
}

impl Manifest {
    /// Object name of the entry: `<commit_id>-<batch_id>`.
    #[must_use]
    pub fn file_id(&self) -> String {
        format!("{}-{}", self.commit_id, self.batch_id)
    }
}

/// Running event-time range, in microseconds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EventSpan {
    field: String,
    min: i64,
    max: i64,
}

impl EventSpan {
    /// Range of the contract's event-time field in `batch`: the
    /// `x-dl-partition_by` field, else the first field stored as a timestamp.
    pub(crate) fn of(contract: &Contract, batch: &RecordBatch) -> Option<Self> {
        let field = contract.partition_by().map(str::to_owned).or_else(|| {
            contract
                .fields()
                .iter()
                .find(|f| f.timestamp)
                .map(|f| f.name.clone())
        })?;
        let column = batch.column_by_name(&field)?;
        if !matches!(
            column.data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, _)
        ) {
            return None;
        }
        let values = column.as_primitive::<TimestampMicrosecondType>();
        if values.null_count() == values.len() {
            return None;
        }
        Some(Self {
            min: arrow::compute::min(values)?,
            max: arrow::compute::max(values)?,
            field,
        })
    }

    /// Widens `span` with `other`.
    pub(crate) fn merge(span: Option<Self>, other: Option<Self>) -> Option<Self> {
        match (span, other) {
            (Some(a), Some(b)) => Some(Self {
                min: a.min.min(b.min),
                max: a.max.max(b.max),
                field: a.field,
            }),
            (a, b) => a.or(b),
        }
    }

    pub(crate) fn range(&self) -> EventRange {
        let text = |micros: i64| {
            DateTime::<Utc>::from_timestamp_micros(micros)
                .map(|t| t.to_rfc3339_opts(SecondsFormat::Micros, true))
                .unwrap_or_default()
        };
        EventRange {
            field: self.field.clone(),
            min: text(self.min),
            max: text(self.max),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::ContractRegistry;
    use crate::error::DatalakeError;
    use crate::pipeline::{BronzePipeline, IngestRequest, PipelineConfig, RecordSource};
    use crate::storage::{InMemoryStorage, ObjectStorage};
    use crate::stream::StreamOptions;
    use crate::tenant::{ObjectPath, TenantId, TenantProfile};
    use async_trait::async_trait;
    use bytes::Bytes;
    use serde_json::{json, Value};
    use std::sync::{Arc, Mutex};

    const CONTRACT: &str = "app/log/1.0.0";

    /// In-memory store that records the order in which objects become visible.
    #[derive(Default)]
    struct Recording {
        inner: InMemoryStorage,
        order: Mutex<Vec<String>>,
    }

    impl Recording {
        fn done(&self, path: &ObjectPath) {
            self.order.lock().unwrap().push(path.uri.clone());
        }
        fn order(&self) -> Vec<String> {
            self.order.lock().unwrap().clone()
        }
        fn manifests(&self) -> Vec<Manifest> {
            let mut keys: Vec<String> = self
                .inner
                .keys()
                .into_iter()
                .filter(|k| k.contains("/_manifests/"))
                .collect();
            keys.sort();
            keys.iter()
                .map(|k| serde_json::from_slice(&self.inner.get(k).unwrap().body).unwrap())
                .collect()
        }
    }

    #[async_trait]
    impl ObjectStorage for Recording {
        async fn put(&self, path: &ObjectPath, body: Bytes, ct: &str) -> Result<(), DatalakeError> {
            self.inner.put(path, body, ct).await?;
            self.done(path);
            Ok(())
        }
        async fn create_multipart(
            &self,
            path: &ObjectPath,
            ct: &str,
        ) -> Result<String, DatalakeError> {
            self.inner.create_multipart(path, ct).await
        }
        async fn upload_part(
            &self,
            path: &ObjectPath,
            id: &str,
            n: i32,
            body: Bytes,
        ) -> Result<String, DatalakeError> {
            self.inner.upload_part(path, id, n, body).await
        }
        async fn complete_multipart(
            &self,
            path: &ObjectPath,
            id: &str,
            parts: Vec<(i32, String)>,
        ) -> Result<(), DatalakeError> {
            self.inner.complete_multipart(path, id, parts).await?;
            self.done(path);
            Ok(())
        }
        async fn abort_multipart(&self, path: &ObjectPath, id: &str) -> Result<(), DatalakeError> {
            self.inner.abort_multipart(path, id).await
        }
    }

    fn pipeline(store: &Arc<Recording>, manifests: bool) -> Arc<BronzePipeline> {
        let mut r = ContractRegistry::default();
        r.register(&json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object", "x-dl-domain": "app", "x-dl-datatype": "log",
            "x-dl-schema_version": "1.0.0", "required": ["id"],
            "properties": {
                "id": {"type": "string", "pattern": "^r"},
                "at": {"type": "string", "format": "date-time", "x-dl-type": "timestamp"},
                "msg": {"type": "string"}
            }
        }))
        .unwrap();
        let config = PipelineConfig {
            max_future_skew: None,
            manifests,
            ..PipelineConfig::default()
        };
        let storage: Arc<dyn ObjectStorage> = Arc::<Recording>::clone(store);
        Arc::new(BronzePipeline::new(Arc::new(r), storage, config).unwrap())
    }

    fn tenant() -> Arc<TenantProfile> {
        Arc::new(
            TenantProfile::for_bucket(TenantId::new("acme-001").unwrap(), "lake")
                .unwrap()
                .build()
                .unwrap(),
        )
    }

    fn rows(start: usize, n: usize) -> Vec<Value> {
        (start..start + n)
            .map(|i| {
                let id = if i % 10 == 9 { format!("bad{i}") } else { format!("r{i}") };
                json!({"id": id, "at": format!("2026-03-01T{:02}:00:00Z", i % 24), "msg": "x".repeat(200)})
            })
            .collect()
    }

    /// Every object an entry lists became visible before the entry.
    fn listed_before(store: &Recording, entry: &Manifest) {
        let order = store.order();
        let at = |uri: &str| order.iter().position(|u| u == uri).unwrap_or(usize::MAX);
        let me = order
            .iter()
            .position(|u| u.ends_with(&format!("{}.json", entry.file_id())))
            .expect("manifest written");
        for o in entry.data.iter().chain(&entry.dead_letters) {
            assert!(at(&o.uri) < me, "{} after its manifest", o.uri);
        }
    }

    #[tokio::test]
    async fn ingest_writes_one_entry_after_its_objects() {
        let store = Arc::new(Recording::default());
        let p = pipeline(&store, true);
        let prepared = p
            .prepare(IngestRequest::new(
                tenant(),
                CONTRACT,
                RecordSource::Values(rows(0, 30)),
            ))
            .unwrap();
        let report = p.commit(&prepared).await.unwrap();
        // A retried upload rewrites the same entry.
        p.commit(&prepared).await.unwrap();
        let entries = store.manifests();
        assert_eq!(entries.len(), 1);
        let m = &entries[0];
        assert_eq!(m.batch_id, report.batch_id);
        assert_eq!(
            (m.records_received, m.records_ok, m.records_quarantined),
            (30, 27, 3)
        );
        assert_eq!(m.data.len(), 1);
        assert_eq!(m.data[0].uri, report.data_uri.clone().unwrap());
        assert_eq!(m.dead_letters.len(), 1);
        assert_eq!(m.contract, CONTRACT);
        let range = m.event_time.clone().unwrap();
        assert_eq!(
            (range.field.as_str(), range.min.as_str(), range.max.as_str()),
            (
                "at",
                "2026-03-01T00:00:00.000000Z",
                "2026-03-01T23:00:00.000000Z"
            )
        );
        let key = store
            .inner
            .keys()
            .into_iter()
            .find(|k| k.contains("/_manifests/"))
            .unwrap();
        assert!(
            key.starts_with("lake/bronze/_manifests/tenant_id=acme-001/data_type=log/date="),
            "{key}"
        );
        assert!(key.ends_with(&format!("{}.json", m.file_id())));
        listed_before(&store, m);
    }

    #[tokio::test]
    async fn no_entries_when_off() {
        let store = Arc::new(Recording::default());
        pipeline(&store, false)
            .ingest(IngestRequest::new(
                tenant(),
                CONTRACT,
                RecordSource::Values(rows(0, 5)),
            ))
            .await
            .unwrap();
        assert_eq!(store.manifests(), Vec::<Manifest>::new());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stream_entries_cover_the_stream_in_order() {
        let store = Arc::new(Recording::default());
        let p = pipeline(&store, true);
        let options = StreamOptions {
            target_object_bytes: 64 * 1024,
            part_bytes: 16 * 1024,
            row_group_bytes: 16 * 1024,
            chunk_rows: 500,
            dead_letter_bytes: 4 * 1024,
            ..StreamOptions::default()
        };
        let report = tokio::task::spawn_blocking(move || {
            let mut stream = p.stream(tenant(), CONTRACT, None, options).unwrap();
            for c in 0..3 {
                stream
                    .push(RecordSource::Values(rows(c * 1_000, 1_000)))
                    .unwrap();
                stream.flush().unwrap();
            }
            stream.push(RecordSource::Values(rows(3_000, 500))).unwrap();
            stream.finish().unwrap()
        })
        .await
        .unwrap();
        let entries = store.manifests();
        assert_eq!(entries.len(), 4, "three flushes and the finish");
        let mut uris: Vec<String> = entries
            .iter()
            .flat_map(|m| m.data.iter().map(|o| o.uri.clone()))
            .collect();
        uris.sort();
        let mut expected = report.data_uris.clone();
        expected.sort();
        assert_eq!(uris, expected, "every object listed exactly once");
        let sum = |f: fn(&Manifest) -> usize| entries.iter().map(f).sum::<usize>();
        assert_eq!(sum(|m| m.records_received), report.records_received);
        assert_eq!(sum(|m| m.records_ok), report.records_ok);
        assert_eq!(sum(|m| m.records_quarantined), report.records_quarantined);
        assert!(entries.iter().all(|m| m.batch_id == report.stream_id));
        let ids: Vec<_> = entries.iter().map(|m| m.commit_id.clone()).collect();
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted, "entries sort by commit");
        for m in &entries {
            listed_before(&store, m);
            for o in &m.data {
                let body = store
                    .inner
                    .get(o.uri.strip_prefix("s3://").unwrap())
                    .unwrap()
                    .body;
                assert_eq!(o.bytes, body.len() as u64, "sizes are exact");
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn continuous_streams_write_an_entry_per_age_close() {
        let store = Arc::new(Recording::default());
        let p = pipeline(&store, true);
        let options = StreamOptions {
            max_object_age: Some(std::time::Duration::ZERO),
            ..StreamOptions::default()
        };
        let report = tokio::task::spawn_blocking(move || {
            let mut stream = p.stream(tenant(), CONTRACT, None, options).unwrap();
            for c in 0..3 {
                // JSON chunks are validated as they come, so each push opens
                // an object and the next one closes it by age.
                stream.push(RecordSource::Values(rows(c * 10, 10))).unwrap();
            }
            stream.finish().unwrap()
        })
        .await
        .unwrap();
        let entries = store.manifests();
        assert_eq!(entries.len(), 3, "two age closes and the finish");
        assert_eq!(
            entries.iter().map(|m| m.records_received).sum::<usize>(),
            report.records_received
        );
        for m in &entries {
            assert_eq!(m.data.len(), 1);
            listed_before(&store, m);
        }
    }
}
