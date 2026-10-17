//! Dead-letter entries for quarantined records.
//!
//! Rejected records of one ingest call are grouped by error code and written
//! as newline-delimited JSON under the dead-letter prefix, one object per
//! code. Payloads get the same PII protection as the Bronze data of the call
//! (the tenant profile's scheme), so a dead-letter object never holds a value
//! the Bronze layer would not hold.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::DatalakeError;

/// One quarantined record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeadLetter {
    /// Ingest call that received the record.
    pub batch_id: String,
    /// Position of the record in the submitted batch.
    pub record_index: usize,
    /// Tenant the record was submitted for.
    pub tenant_id: String,
    /// Contract key (`domain/datatype/version`).
    pub contract: String,
    /// Contract data type.
    pub data_type: String,
    /// Validation level that rejected the record.
    pub validation_level: u8,
    /// Machine-readable error code.
    pub error_code: String,
    /// Reason, free of record values.
    pub error_message: String,
    /// RFC 3339 time of quarantine.
    pub quarantined_at: String,
    /// SDK version that quarantined the record.
    pub sdk_version: String,
    /// Size of the record as submitted, in bytes.
    pub payload_bytes: usize,
    /// PII scheme applied to `payload` (`none` when stored as received).
    pub pii_protection: String,
    /// The record, PII protected as in Bronze; `null` when it was not a JSON
    /// object or exceeded the size limit.
    pub payload: Option<Value>,
}

/// Encodes entries as newline-delimited JSON.
pub(crate) fn encode_ndjson<'a>(
    entries: impl IntoIterator<Item = &'a DeadLetter>,
) -> Result<Bytes, DatalakeError> {
    let mut out = Vec::new();
    for entry in entries {
        serde_json::to_writer(&mut out, entry)
            .map_err(|e| DatalakeError::storage("dlq", format!("cannot encode entry: {e}")))?;
        out.push(b'\n');
    }
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn entry(i: usize) -> DeadLetter {
        DeadLetter {
            batch_id: "B".into(),
            record_index: i,
            tenant_id: "t-1".into(),
            contract: "d/t/1".into(),
            data_type: "t".into(),
            validation_level: 3,
            error_code: "ERR_SCHEMA_VIOLATION".into(),
            error_message: "/x: wrong type".into(),
            quarantined_at: "2026-10-12T00:00:00Z".into(),
            sdk_version: "0.1.0".into(),
            payload_bytes: 10,
            pii_protection: "none".into(),
            payload: Some(json!({"x": 1})),
        }
    }

    #[test]
    fn ndjson_roundtrip() {
        let entries = [entry(0), entry(1)];
        let bytes = encode_ndjson(&entries).unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        let back: Vec<DeadLetter> = text
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(back, entries);
        assert!(text.ends_with('\n'));
    }
}
