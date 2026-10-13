//! Record validation stages.
//!
//! | Level | Stage | Where |
//! |-------|-------|-------|
//! | 0 | Contract boot (compile every schema, bind hooks) | [`crate::contracts::ContractRegistry`] |
//! | 1 | Tenant identifier | [`crate::tenant::TenantId`] |
//! | 2 | Payload: JSON object, size limit, reserved envelope fields | pipeline |
//! | 3 | JSON Schema contract (formats asserted) and undeclared fields in strict mode | [`crate::contracts::Contract`] |
//! | 4 | Pre-ingest hook supplied by the host | [`hooks::RecordHook`] |
//! | 5 | Semantic rules: future-timestamp guard on `date-time` fields | [`semantic`] |
//! | 6 | PII pseudonymization of fields marked in the contract (optional, on by default) | [`pii::PiiHasher`] |
//! | 7 | Value fits the Arrow column type derived from the contract | pipeline |
//! | 8 | Lineage envelope columns | [`crate::envelope`] |
//!
//! Levels 2 to 7 never fail the batch: a record that fails is quarantined to
//! the dead-letter prefix (with PII fields hashed when masking is on), unless the
//! pipeline runs with [`crate::pipeline::InvalidRecordPolicy::Reject`].

pub mod hooks;
pub mod pii;
pub mod semantic;

/// Numeric validation levels used in dead-letter entries and errors.
pub mod level {
    /// Payload decoding and structural checks.
    pub const PAYLOAD: u8 = 2;
    /// JSON Schema contract.
    pub const SCHEMA: u8 = 3;
    /// Host pre-ingest hook.
    pub const HOOK: u8 = 4;
    /// Semantic rules.
    pub const SEMANTIC: u8 = 5;
    /// Arrow type coercion.
    pub const TYPE_COERCION: u8 = 7;
}

/// Machine-readable error codes. They become an `error_code=` partition of
/// the dead-letter key, so every code matches `[A-Z0-9_]{1,64}`.
pub mod codes {
    /// The record is not a JSON object.
    pub const RECORD_NOT_OBJECT: &str = "ERR_RECORD_NOT_OBJECT";
    /// The serialized record exceeds the configured size limit.
    pub const RECORD_TOO_LARGE: &str = "ERR_RECORD_TOO_LARGE";
    /// The record carries a field that starts with the envelope prefix.
    pub const RESERVED_FIELD: &str = "ERR_RESERVED_FIELD";
    /// The record violates its JSON Schema.
    pub const SCHEMA_VIOLATION: &str = "ERR_SCHEMA_VIOLATION";
    /// The record carries a field the strict contract does not declare.
    pub const UNDECLARED_FIELD: &str = "ERR_UNDECLARED_FIELD";
    /// The hook rejected the record without a usable code.
    pub const HOOK_REJECTED: &str = "ERR_HOOK_REJECTED";
    /// The hook raised an exception.
    pub const HOOK_EXCEPTION: &str = "ERR_HOOK_EXCEPTION";
    /// The hook returned something other than an accept/reject result.
    pub const HOOK_INVALID_RESULT: &str = "ERR_HOOK_INVALID_RESULT";
    /// A `date-time` field lies further in the future than allowed.
    pub const FUTURE_TIMESTAMP: &str = "ERR_FUTURE_TIMESTAMP";
    /// A value does not fit the column type derived from the contract.
    pub const TYPE_COERCION: &str = "ERR_TYPE_COERCION";
}

/// Why a single record was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejection {
    /// Validation level that refused the record.
    pub level: u8,
    /// Code matching `[A-Z0-9_]{1,64}`.
    pub code: String,
    /// Reason without record values.
    pub message: String,
}

impl Rejection {
    /// Builds a rejection, normalizing `code` so it is safe as a storage key segment.
    pub fn new(level: u8, code: &str, message: impl Into<String>) -> Self {
        Self {
            level,
            code: sanitize_code(code, codes::HOOK_REJECTED),
            message: message.into(),
        }
    }
}

/// Uppercases `code`, replaces anything outside `[A-Z0-9_]` with `_` and
/// truncates to 64 characters. Returns `fallback` when nothing usable remains.
pub(crate) fn sanitize_code(code: &str, fallback: &str) -> String {
    let cleaned: String = code
        .chars()
        .map(|c| {
            let c = c.to_ascii_uppercase();
            if c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .take(64)
        .collect();
    if cleaned.trim_matches('_').is_empty() {
        fallback.to_owned()
    } else {
        cleaned
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_keeps_valid_codes() {
        assert_eq!(sanitize_code("ERR_X_1", "F"), "ERR_X_1");
    }

    #[test]
    fn sanitize_normalizes_path_like_codes() {
        assert_eq!(sanitize_code("../err=x/y", "F"), "___ERR_X_Y");
        assert_eq!(sanitize_code("bad code", "F"), "BAD_CODE");
    }

    #[test]
    fn sanitize_falls_back_when_empty() {
        assert_eq!(sanitize_code("", "FALLBACK"), "FALLBACK");
        assert_eq!(sanitize_code("///", "FALLBACK"), "FALLBACK");
    }

    #[test]
    fn sanitize_truncates_to_64() {
        assert_eq!(sanitize_code(&"A".repeat(100), "F").len(), 64);
    }
}
