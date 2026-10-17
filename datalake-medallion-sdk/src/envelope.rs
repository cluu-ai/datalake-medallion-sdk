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
//! | `_dl_pii_masking` | utf8 | PII scheme applied: `sha256`, `hmac-sha256` or `none` |
//! | `_dl_sdk_version` | utf8 | version of this SDK |
//!
//! Contracts in `permissive` or `schema_on_read` mode add `_dl_extra`, a JSON
//! text column holding undeclared fields. Payload fields may not start with
//! the prefix: such records are rejected at Level 2.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, TimeUnit};
use chrono::{DateTime, Utc};
use ulid::{Generator, Overflow};

use crate::error::DatalakeError;

/// Default envelope column prefix.
pub const DEFAULT_ENVELOPE_PREFIX: &str = "_dl_";

const SUFFIXES: [&str; 9] = [
    "ingest_id",
    "batch_id",
    "tenant_id",
    "data_type",
    "schema_version",
    "schema_hash",
    "received_at",
    "pii_masking",
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
                let dt = if *s == "received_at" {
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
                } else {
                    DataType::Utf8
                };
                Field::new(self.column(s), dt, false)
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
    pub pii_masking: &'a str,
}

impl Envelope<'_> {
    /// Envelope arrays for `rows` rows, matching [`EnvelopePrefix::fields`].
    pub(crate) fn columns(&self, rows: usize) -> Vec<ArrayRef> {
        let constant = |v: &str| -> ArrayRef {
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(v, rows)))
        };
        let mut generator = Generator::new();
        // More than 2^80 ids in one millisecond overflows the random part; carrying
        // into the next millisecond keeps the column strictly increasing.
        let ingest_ids = StringArray::from_iter_values((0..rows).map(|_| {
            generator
                .generate()
                .unwrap_or_else(Overflow::commit_overflow_increment)
                .to_string()
        }));
        let received = TimestampMicrosecondArray::from_iter_values(std::iter::repeat_n(
            self.received_at.timestamp_micros(),
            rows,
        ))
        .with_timezone("UTC");
        vec![
            Arc::new(ingest_ids),
            constant(self.batch_id),
            constant(self.tenant_id),
            constant(self.data_type),
            constant(self.schema_version),
            constant(self.schema_hash),
            Arc::new(received),
            constant(self.pii_masking),
            constant(env!("CARGO_PKG_VERSION")),
        ]
    }
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
            pii_masking: "none",
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
