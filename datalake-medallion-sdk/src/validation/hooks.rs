//! Level 4: pre-ingest hooks supplied by the host application.
//!
//! A hook sees a record after it passed its contract and may accept it
//! (optionally rewritten) or reject it. A rewritten record is validated
//! against the contract again, so a hook cannot smuggle invalid data or
//! reserved envelope fields into the lake.

use serde_json::{Map, Value};

/// Decision returned by a [`RecordHook`].
#[derive(Debug, Clone, PartialEq)]
pub enum HookOutcome {
    /// Keep the record, using this (possibly modified) payload.
    Accept(Map<String, Value>),
    /// Quarantine the record.
    Reject {
        /// Error code; normalized to `[A-Z0-9_]{1,64}`.
        code: String,
        /// Reason recorded in the dead-letter entry. Must not contain PII.
        reason: String,
    },
    /// The hook itself failed; the record is quarantined with `ERR_HOOK_EXCEPTION`.
    Failed {
        /// Description of the failure, without record values.
        message: String,
    },
}

/// Port for host-defined record checks or enrichment.
pub trait RecordHook: Send + Sync {
    /// Inspects one record.
    fn apply(&self, record: Map<String, Value>) -> HookOutcome;
}

impl<F> RecordHook for F
where
    F: Fn(Map<String, Value>) -> HookOutcome + Send + Sync,
{
    fn apply(&self, record: Map<String, Value>) -> HookOutcome {
        self(record)
    }
}
