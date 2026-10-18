//! Error type for the SDK and its mapping to Python exceptions.
//!
//! Messages never contain record values: schema errors are rendered with
//! instance values masked, and PII fields are hashed before any record is
//! serialized into a dead-letter entry.

#[cfg(feature = "python")]
use pyo3::prelude::*;
use thiserror::Error;

#[cfg(any(feature = "python", test))]
use crate::validation::level;
use crate::validation::Rejection;

/// Every error the SDK can return.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DatalakeError {
    /// A contract could not be loaded or compiled (Level 0).
    #[error("contract boot error: {message}")]
    ContractBoot {
        /// What failed and where.
        message: String,
    },

    /// The requested contract key is not registered.
    #[error("unknown contract '{key}'")]
    UnknownContract {
        /// The key that was requested (`domain/datatype/version`).
        key: String,
    },

    /// The tenant identifier failed validation (Level 1).
    #[error("invalid tenant id {tenant_id:?}: {reason}")]
    TenantViolation {
        /// The rejected identifier, debug-escaped when displayed.
        tenant_id: String,
        /// The rule that failed.
        reason: String,
    },

    /// The batch as a whole could not be decoded (Level 2).
    #[error("malformed payload: {reason}")]
    MalformedPayload {
        /// Why the batch could not be decoded.
        reason: String,
    },

    /// A record failed validation and the pipeline runs in fail-fast mode.
    #[error("record {record_index} rejected at level {level} [{code}]: {message}")]
    RecordRejected {
        /// Position of the record inside the submitted batch.
        record_index: usize,
        /// Validation level that rejected the record.
        level: u8,
        /// Machine-readable error code.
        code: String,
        /// Human-readable reason, free of record values.
        message: String,
    },

    /// Object storage or encoding failure.
    #[error("storage error [{backend}]: {message}")]
    Storage {
        /// Component that failed (`s3`, `parquet`, ...).
        backend: &'static str,
        /// Description of the failure.
        message: String,
    },

    /// PII protection failed (Level 6); nothing was written.
    #[error("PII protection error: {message}")]
    Protection {
        /// What failed, free of record values.
        message: String,
    },

    /// Invalid SDK configuration.
    #[error("configuration error: {message}")]
    Configuration {
        /// What is wrong with the configuration.
        message: String,
    },
}

impl DatalakeError {
    pub(crate) fn config(message: impl Into<String>) -> Self {
        Self::Configuration {
            message: message.into(),
        }
    }

    pub(crate) fn boot(message: impl Into<String>) -> Self {
        Self::ContractBoot {
            message: message.into(),
        }
    }

    pub(crate) fn protection(message: impl Into<String>) -> Self {
        Self::Protection {
            message: message.into(),
        }
    }

    pub(crate) fn storage(backend: &'static str, message: impl Into<String>) -> Self {
        Self::Storage {
            backend,
            message: message.into(),
        }
    }

    pub(crate) fn rejected(record_index: usize, rejection: &Rejection) -> Self {
        Self::RecordRejected {
            record_index,
            level: rejection.level,
            code: rejection.code.clone(),
            message: rejection.message.clone(),
        }
    }
}

/// Python exception classes exposed by the native module.
#[cfg(feature = "python")]
pub mod exceptions {
    use pyo3::create_exception;
    use pyo3::exceptions::PyException;

    create_exception!(
        datalake_medallion_sdk,
        DatalakeError,
        PyException,
        "Base class for every exception raised by datalake-medallion-sdk."
    );
    create_exception!(
        datalake_medallion_sdk,
        ContractBootError,
        DatalakeError,
        "A data contract could not be loaded, compiled or bound (Level 0)."
    );
    create_exception!(
        datalake_medallion_sdk,
        ConfigurationError,
        DatalakeError,
        "Invalid SDK configuration or unknown contract key."
    );
    create_exception!(
        datalake_medallion_sdk,
        TenantViolationError,
        DatalakeError,
        "The tenant identifier failed validation (Level 1)."
    );
    create_exception!(
        datalake_medallion_sdk,
        MalformedPayloadError,
        DatalakeError,
        "The batch could not be decoded, or a record is malformed (Level 2)."
    );
    create_exception!(
        datalake_medallion_sdk,
        RecordRejectedError,
        DatalakeError,
        "A record failed validation while the client runs with on_invalid='raise'."
    );
    create_exception!(
        datalake_medallion_sdk,
        SchemaViolationError,
        RecordRejectedError,
        "A record violates its JSON Schema contract (Level 3)."
    );
    create_exception!(
        datalake_medallion_sdk,
        HookError,
        RecordRejectedError,
        "A pre-ingest hook rejected a record or raised (Level 4)."
    );
    create_exception!(
        datalake_medallion_sdk,
        SemanticViolationError,
        RecordRejectedError,
        "A record failed a semantic rule such as the future-timestamp guard (Level 5)."
    );
    create_exception!(
        datalake_medallion_sdk,
        TypeCoercionError,
        RecordRejectedError,
        "A value cannot be stored with the column type derived from the contract (Level 7)."
    );
    create_exception!(
        datalake_medallion_sdk,
        StorageError,
        DatalakeError,
        "Encoding or object storage failure. Safe to retry."
    );
    create_exception!(
        datalake_medallion_sdk,
        ProtectionError,
        DatalakeError,
        "PII protection failed (Level 6); nothing was written."
    );
}

#[cfg(feature = "python")]
impl From<DatalakeError> for PyErr {
    fn from(err: DatalakeError) -> Self {
        use exceptions as ex;
        let msg = err.to_string();
        match err {
            DatalakeError::ContractBoot { .. } => ex::ContractBootError::new_err(msg),
            DatalakeError::UnknownContract { .. } | DatalakeError::Configuration { .. } => {
                ex::ConfigurationError::new_err(msg)
            }
            DatalakeError::TenantViolation { .. } => ex::TenantViolationError::new_err(msg),
            DatalakeError::MalformedPayload { .. } => ex::MalformedPayloadError::new_err(msg),
            DatalakeError::RecordRejected { level: lvl, .. } => match lvl {
                level::PAYLOAD => ex::MalformedPayloadError::new_err(msg),
                level::SCHEMA => ex::SchemaViolationError::new_err(msg),
                level::HOOK => ex::HookError::new_err(msg),
                level::SEMANTIC => ex::SemanticViolationError::new_err(msg),
                level::TYPE_COERCION => ex::TypeCoercionError::new_err(msg),
                _ => ex::RecordRejectedError::new_err(msg),
            },
            DatalakeError::Storage { .. } => ex::StorageError::new_err(msg),
            DatalakeError::Protection { .. } => ex::ProtectionError::new_err(msg),
        }
    }
}

/// Registers the exception classes on the native module.
#[cfg(feature = "python")]
pub(crate) fn register_exceptions(m: &Bound<'_, PyModule>) -> PyResult<()> {
    use exceptions as ex;
    let py = m.py();
    m.add("DatalakeError", py.get_type::<ex::DatalakeError>())?;
    m.add("ContractBootError", py.get_type::<ex::ContractBootError>())?;
    m.add(
        "ConfigurationError",
        py.get_type::<ex::ConfigurationError>(),
    )?;
    m.add(
        "TenantViolationError",
        py.get_type::<ex::TenantViolationError>(),
    )?;
    m.add(
        "MalformedPayloadError",
        py.get_type::<ex::MalformedPayloadError>(),
    )?;
    m.add(
        "RecordRejectedError",
        py.get_type::<ex::RecordRejectedError>(),
    )?;
    m.add(
        "SchemaViolationError",
        py.get_type::<ex::SchemaViolationError>(),
    )?;
    m.add("HookError", py.get_type::<ex::HookError>())?;
    m.add(
        "SemanticViolationError",
        py.get_type::<ex::SemanticViolationError>(),
    )?;
    m.add("TypeCoercionError", py.get_type::<ex::TypeCoercionError>())?;
    m.add("StorageError", py.get_type::<ex::StorageError>())?;
    m.add("ProtectionError", py.get_type::<ex::ProtectionError>())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_violation_escapes_control_characters() {
        let err = DatalakeError::TenantViolation {
            tenant_id: "bad\nid".into(),
            reason: "control character".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("bad\\nid"), "{msg}");
        assert!(!msg.contains('\n'));
    }

    #[test]
    fn rejected_error_carries_level_and_code() {
        let rejection = Rejection::new(level::SCHEMA, "ERR_SCHEMA_VIOLATION", "/age: wrong type");
        let err = DatalakeError::rejected(7, &rejection);
        let msg = err.to_string();
        assert!(msg.contains("record 7"));
        assert!(msg.contains("level 3"));
        assert!(msg.contains("ERR_SCHEMA_VIOLATION"));
    }
}
