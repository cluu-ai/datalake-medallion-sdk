//! Tenant isolation (Level 1) and tenant profiles.
//!
//! Every write requires a validated [`TenantId`]; a raw string cannot reach
//! storage. Each tenant's [`TenantProfile`] decides where its data lives per
//! medallion layer and how its PII is protected, and every object key keeps
//! the tenant in a `tenant_id=` Hive partition.

pub mod id;
pub mod path;
pub mod profile;

pub use id::TenantId;
pub use path::{ObjectPath, PartitionOrder};
pub use profile::{
    Layer, StorageLocation, TenantDirectory, TenantProfile, TenantProfileBuilder, TenantRegistry,
};
