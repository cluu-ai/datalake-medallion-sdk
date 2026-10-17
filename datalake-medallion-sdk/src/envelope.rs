//! Level 8: lineage envelope written as flat columns on every Bronze row.
//!
//! With the default prefix `_dl_` the columns are:
//!
//! | Column | Type | Value |
//! |--------|------|-------|
//! | `_dl_ingest_id` | utf8 | ULID unique per row, monotonic within a batch |
//! | `_dl_batch_id` | utf8 | ULID of the ingest call; also names the Parquet and dead-letter objects |
//! | `_dl_tenant_id` | utf8 | validated tenant |
//! | `_dl_data_type` | utf8 | contract data type |
//! | `_dl_schema_version` | utf8 | contract version |
//! | `_dl_schema_hash` | utf8 | SHA-256 of the contract document |
//! | `_dl_received_at` | timestamp(µs, UTC) | ingest time |
//! | `_dl_pii_protection` | utf8 | PII scheme of the tenant profile: `none` (default), `sha256`, `hmac-sha256`, `aes-256-gcm` or a custom name |
//! | `_dl_pii_key_ref` | utf8, nullable | key reference of reversible schemes (e.g. the KMS-encrypted data key); null otherwise |
//! | `_dl_sdk_version` | utf8 | version of this SDK |
//!
//! Contracts in `permissive` or `schema_on_read` mode add `_dl_extra`, a JSON
//! text column holding undeclared fields. Payload fields may not start with
//! the prefix: such records are rejected at Level 2.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, DictionaryArray, Int8Array, StringArray, StringBuilder, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Int8Type, TimeUnit};
use chrono::{DateTime, Utc};
use ulid::{Generator, Overflow, ULID_LEN};

use crate::error::DatalakeError;

/// Default envelope column prefix.
pub const DEFAULT_ENVELOPE_PREFIX: &str = "_dl_";

const SUFFIXES: [&str; 10] = [
    "ingest_id",
    "batch_id",
    "tenant_id",
    "data_type",
    "schema_version",
    "schema_hash",
    "received_at",
    "pii_protection",
    "pii_key_ref",
    "sdk_version",
];

/// Validated envelope column prefix: `[a-z_][a-z0-9_]{0,15}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvelopePrefix(String);

impl Default for EnvelopePrefix {
    fn default() -> Self {
        Self(DEFAULT_ENVELOPE_PREFIX.to_owned())
    }
}

impl EnvelopePrefix {
    /// Validates and wraps a prefix.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] unless `prefix` matches `[a-z_][a-z0-9_]{0,15}`.
    pub fn new(prefix: &str) -> Result<Self, DatalakeError> {
        let mut bytes = prefix.bytes();
        let ok = (1..=16).contains(&prefix.len())
            && bytes
                .next()
                .is_some_and(|b| b.is_ascii_lowercase() || b == b'_')
            && bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
        if ok {
            Ok(Self(prefix.to_owned()))
        } else {
            Err(DatalakeError::config(format!(
                "envelope prefix {prefix:?} must match [a-z_][a-z0-9_]{{0,15}}"
            )))
        }
    }

    /// The prefix string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether a payload field name collides with the reserved namespace.
    #[must_use]
    pub fn is_reserved(&self, field: &str) -> bool {
        field.starts_with(&self.0)
    }

    /// Full name of an envelope column, e.g. `column("tenant_id")`.
    #[must_use]
    pub fn column(&self, suffix: &str) -> String {
        format!("{}{suffix}", self.0)
    }

    /// Name of the column holding undeclared fields.
    #[must_use]
    pub fn extra_column(&self) -> String {
        self.column("extra")
    }

    /// Arrow fields appended to every Bronze batch, in column order.
    #[must_use]
    pub fn fields(&self) -> Vec<Field> {
        SUFFIXES
            .iter()
            .map(|s| {
                let dt = match *s {
                    "received_at" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    "ingest_id" => DataType::Utf8,
                    _ => constant_type(),
                };
                Field::new(self.column(s), dt, *s == "pii_key_ref")
            })
            .collect()
    }
}

/// Per-batch envelope values.
#[derive(Debug, Clone)]
pub(crate) struct Envelope<'a> {
    pub batch_id: &'a str,
    pub tenant_id: &'a str,
    pub data_type: &'a str,
    pub schema_version: &'a str,
    pub schema_hash: &'a str,
    pub received_at: DateTime<Utc>,
    pub pii_protection: &'a str,
    pub pii_key_ref: Option<&'a str>,
}

impl Envelope<'_> {
    /// Envelope arrays for `rows` rows, matching [`EnvelopePrefix::fields`].
    pub(crate) fn columns(&self, rows: usize) -> Vec<ArrayRef> {
        let constant = |v: Option<&str>| -> ArrayRef { Arc::new(constant_column(v, rows)) };
        let mut generator = Generator::new();
        let mut ingest_ids = StringBuilder::with_capacity(rows, rows * ULID_LEN);
        let mut text = [0u8; ULID_LEN];
        for _ in 0..rows {
            // More than 2^80 ids in one millisecond overflows the random part;
            // carrying into the next millisecond keeps the column increasing.
            let id = generator
                .generate()
                .unwrap_or_else(Overflow::commit_overflow_increment);
            ingest_ids.append_value(id.array_to_str(&mut text));
        }
        let received = TimestampMicrosecondArray::from_iter_values(std::iter::repeat_n(
            self.received_at.timestamp_micros(),
            rows,
        ))
        .with_timezone("UTC");
        vec![
            Arc::new(ingest_ids.finish()),
            constant(Some(self.batch_id)),
            constant(Some(self.tenant_id)),
            constant(Some(self.data_type)),
            constant(Some(self.schema_version)),
            constant(Some(self.schema_hash)),
            Arc::new(received),
            constant(Some(self.pii_protection)),
            constant(self.pii_key_ref),
            constant(Some(env!("CARGO_PKG_VERSION"))),
        ]
    }
}

/// Arrow type of the columns that hold one value per batch: a one-entry
/// dictionary costs one byte per row instead of a copy of the value. Parquet
/// files store them as plain strings (see `ParquetEncoder`).
fn constant_type() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8))
}

/// `rows` copies of `value` (or nulls) as a one-entry dictionary.
fn constant_column(value: Option<&str>, rows: usize) -> DictionaryArray<Int8Type> {
    let keys = match value {
        Some(_) => Int8Array::from_value(0, rows),
        None => Int8Array::new_null(rows),
    };
    let values = StringArray::from_iter_values(value);
    DictionaryArray::new(keys, Arc::new(values))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    #[test]
    fn accepts_valid_prefixes() {
        for p in ["_dl_", "_acme_", "meta_", "_", "a1_"] {
            assert!(EnvelopePrefix::new(p).is_ok(), "{p}");
        }
    }

    #[test]
    fn rejects_invalid_prefixes() {
        for p in ["", "1x", "_DL_", "dl-", "a.b", "_seventeen_chars_"] {
            assert!(EnvelopePrefix::new(p).is_err(), "{p:?}");
        }
    }

    #[test]
    fn reserved_detection() {
        let p = EnvelopePrefix::default();
        assert!(p.is_reserved("_dl_tenant_id"));
        assert!(!p.is_reserved("dl_tenant"));
    }

    #[test]
    fn columns_match_fields_and_ids_are_unique() {
        let p = EnvelopePrefix::default();
        let env = Envelope {
            batch_id: "B",
            tenant_id: "t-1",
            data_type: "events",
            schema_version: "1.0.0",
            schema_hash: "h",
            received_at: Utc::now(),
            pii_protection: "aes-256-gcm",
            pii_key_ref: Some("kms-blob"),
        };
        let cols = env.columns(100);
        let fields = p.fields();
        assert_eq!(cols.len(), fields.len());
        for (c, f) in cols.iter().zip(&fields) {
            assert_eq!(c.data_type(), f.data_type(), "{}", f.name());
            assert_eq!(c.len(), 100);
        }
        let ids = cols[0].as_any().downcast_ref::<StringArray>().unwrap();
        let unique: std::collections::HashSet<_> = ids.iter().flatten().collect();
        assert_eq!(unique.len(), 100);
        let sorted: Vec<_> = ids.iter().flatten().collect();
        assert!(sorted.windows(2).all(|w| w[0] < w[1]), "monotonic ULIDs");
    }
}
