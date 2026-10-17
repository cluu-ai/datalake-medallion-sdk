//! # datalake-medallion-sdk
//!
//! Contract-enforced Bronze-layer ingestion for medallion data lakes.
//!
//! Every record goes through one pipeline before it reaches object storage:
//! tenant validation, payload checks, JSON Schema contract, optional host
//! hook, semantic rules, optional PII protection (off by default) and Arrow
//! type coercion. Valid records become one Hive-partitioned Parquet object
//! with lineage columns; invalid records are quarantined. Where both go, and
//! how PII is protected, is decided per tenant by its [`tenant::TenantProfile`],
//! which every ingest call carries. See [`validation`] for the level table.
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use _core::contracts::{ContractRegistry, FileSystemSchemaProvider, DEFAULT_EXTENSION_PREFIX};
//! use _core::pipeline::{BronzePipeline, IngestRequest, PipelineConfig, RecordSource};
//! use _core::storage::{S3Options, S3Storage};
//! use _core::tenant::{TenantId, TenantProfile};
//!
//! # async fn run() -> Result<(), _core::error::DatalakeError> {
//! let registry = ContractRegistry::from_provider(
//!     &FileSystemSchemaProvider::new("contracts"),
//!     DEFAULT_EXTENSION_PREFIX,
//! )?;
//! let storage = S3Storage::connect(S3Options::default()).await;
//! let pipeline = BronzePipeline::new(Arc::new(registry), Arc::new(storage), PipelineConfig::default())?;
//! let tenant = TenantProfile::for_bucket(TenantId::new("acme-001")?, "acme-lake")?.build()?;
//! let report = pipeline
//!     .ingest(IngestRequest::new(
//!         Arc::new(tenant),
//!         "security/login_event/1.0.0",
//!         RecordSource::Json(r#"[{"event_id": "e1"}]"#.into()),
//!     ))
//!     .await?;
//! println!("{} ok, {} quarantined", report.records_ok, report.records_quarantined);
//! # Ok(())
//! # }
//! ```

#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]
#![warn(clippy::pedantic)]
#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]
// Noisy pedantic lints that do not improve this API.
#![allow(clippy::module_name_repetitions, clippy::must_use_candidate)]
// pyo3's build script passes /fuse-ld=lld, which link.exe reports as LNK4044.
#![cfg_attr(target_env = "msvc", allow(linker_messages))]

use pyo3::prelude::*;

mod codec;
mod columnar;
pub mod contracts;
pub mod dlq;
pub mod envelope;
pub mod error;
pub mod pipeline;
mod python;
pub mod storage;
pub mod tenant;
pub mod validation;

pub use error::DatalakeError;

/// Native module `datalake_medallion_sdk._core`.
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    python::register(m)
}
