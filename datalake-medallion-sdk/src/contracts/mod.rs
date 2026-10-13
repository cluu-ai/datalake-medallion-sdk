//! Data contracts: discovery ([`SchemaProvider`]) and the compiled, immutable
//! [`ContractRegistry`] that every ingestion goes through.
//!
//! A contract is a JSON Schema document carrying SDK keywords under a
//! configurable extension prefix (default [`DEFAULT_EXTENSION_PREFIX`]):
//!
//! ```json
//! {
//!   "$schema": "https://json-schema.org/draft/2020-12/schema",
//!   "type": "object",
//!   "x-dl-domain": "security",
//!   "x-dl-datatype": "login_event",
//!   "x-dl-schema_version": "1.0.0",
//!   "x-dl-flexibility_mode": "strict",
//!   "required": ["event_id", "occurred_at"],
//!   "properties": {
//!     "event_id": {"type": "string"},
//!     "occurred_at": {"type": "string", "format": "date-time"},
//!     "user_email": {"type": "string", "x-dl-pii": true}
//!   }
//! }
//! ```

pub mod provider;
pub mod registry;

pub use provider::{EmbeddedSchemaProvider, FileSystemSchemaProvider, SchemaEntry, SchemaProvider};
pub use registry::{
    Contract, ContractRegistry, FieldKind, FieldSpec, FlexibilityMode, DEFAULT_EXTENSION_PREFIX,
};
