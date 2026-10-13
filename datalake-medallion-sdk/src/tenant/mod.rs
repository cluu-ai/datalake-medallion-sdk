//! Tenant isolation (Level 1).
//!
//! Every write requires a validated [`TenantId`]; a raw string cannot reach
//! storage. Isolation is enforced through the `tenant_id=` Hive partition of
//! each object key built by [`BronzePathResolver`].

pub mod id;
pub mod path;

pub use id::TenantId;
pub use path::{BronzePathResolver, ObjectPath, PartitionOrder};
