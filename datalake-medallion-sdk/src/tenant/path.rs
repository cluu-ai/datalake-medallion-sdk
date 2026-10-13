//! Hive-partitioned object keys for Bronze data and dead-letter entries.
//!
//! The bucket is shared infrastructure and never contains the tenant; tenant
//! isolation lives in the `tenant_id=` partition of every key:
//!
//! ```text
//! TenantFirst:   <data_prefix>/tenant_id=<t>/data_type=<d>/year=YYYY/month=MM/day=DD/<id>.parquet
//! DataTypeFirst: <data_prefix>/data_type=<d>/tenant_id=<t>/year=YYYY/month=MM/day=DD/<id>.parquet
//! Dead letters:  <dlq_prefix>/tenant_id=<t>/data_type=<d>/error_code=<c>/year=YYYY/month=MM/day=DD/<id>.jsonl
//! ```

use chrono::{DateTime, Utc};

use crate::error::DatalakeError;
use crate::tenant::TenantId;

/// Default key prefix for Bronze data.
pub const DEFAULT_DATA_PREFIX: &str = "bronze";
/// Default key prefix for dead-letter entries.
pub const DEFAULT_DLQ_PREFIX: &str = "dlq";

/// Order of the `tenant_id` and `data_type` partitions in data keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PartitionOrder {
    /// `tenant_id=…/data_type=…`: per-tenant prefix policies and lifecycle rules.
    #[default]
    TenantFirst,
    /// `data_type=…/tenant_id=…`: one table location per data type.
    DataTypeFirst,
}

impl PartitionOrder {
    /// Parses `"tenant_first"` or `"data_type_first"`.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] for any other value.
    pub fn parse(value: &str) -> Result<Self, DatalakeError> {
        match value {
            "tenant_first" => Ok(Self::TenantFirst),
            "data_type_first" => Ok(Self::DataTypeFirst),
            other => Err(DatalakeError::config(format!(
                "partition_order must be \"tenant_first\" or \"data_type_first\", got {other:?}"
            ))),
        }
    }
}

/// A resolved object location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectPath {
    /// Key inside the bucket.
    pub key: String,
    /// `s3://bucket/key`.
    pub uri: String,
}

/// Builds object keys for one bucket.
#[derive(Debug, Clone)]
pub struct BronzePathResolver {
    bucket: String,
    data_prefix: String,
    dlq_prefix: String,
    order: PartitionOrder,
}

impl BronzePathResolver {
    /// Creates a resolver with the default prefixes and partition order.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] when the bucket name is invalid.
    pub fn new(bucket: impl Into<String>) -> Result<Self, DatalakeError> {
        let bucket = bucket.into();
        validate_bucket_name(&bucket)?;
        Ok(Self {
            bucket,
            data_prefix: DEFAULT_DATA_PREFIX.to_owned(),
            dlq_prefix: DEFAULT_DLQ_PREFIX.to_owned(),
            order: PartitionOrder::TenantFirst,
        })
    }

    /// Sets the data prefix.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] when the prefix is not a safe key
    /// prefix or overlaps the dead-letter prefix.
    pub fn with_data_prefix(mut self, prefix: impl Into<String>) -> Result<Self, DatalakeError> {
        let prefix = prefix.into();
        validate_prefix("data_prefix", &prefix)?;
        ensure_disjoint(&prefix, &self.dlq_prefix)?;
        self.data_prefix = prefix;
        Ok(self)
    }

    /// Sets the dead-letter prefix.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] when the prefix is not a safe key
    /// prefix or overlaps the data prefix.
    pub fn with_dlq_prefix(mut self, prefix: impl Into<String>) -> Result<Self, DatalakeError> {
        let prefix = prefix.into();
        validate_prefix("dlq_prefix", &prefix)?;
        ensure_disjoint(&self.data_prefix, &prefix)?;
        self.dlq_prefix = prefix;
        Ok(self)
    }

    /// Sets the partition order of data keys.
    #[must_use]
    pub fn with_partition_order(mut self, order: PartitionOrder) -> Self {
        self.order = order;
        self
    }

    /// Key for a Parquet data object.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] when `data_type` is not a valid slug.
    pub fn data_path(
        &self,
        tenant: &TenantId,
        data_type: &str,
        at: &DateTime<Utc>,
        file_id: &str,
    ) -> Result<ObjectPath, DatalakeError> {
        let slug = data_type_slug(data_type)?;
        let (first, second) = match self.order {
            PartitionOrder::TenantFirst => {
                (format!("tenant_id={tenant}"), format!("data_type={slug}"))
            }
            PartitionOrder::DataTypeFirst => {
                (format!("data_type={slug}"), format!("tenant_id={tenant}"))
            }
        };
        let key = format!(
            "{}/{first}/{second}/{}/{file_id}.parquet",
            self.data_prefix,
            date_partitions(at)
        );
        Ok(self.object(key))
    }

    /// Key for a newline-delimited JSON dead-letter object.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] when `data_type` or `error_code` is invalid.
    pub fn dlq_path(
        &self,
        tenant: &TenantId,
        data_type: &str,
        error_code: &str,
        at: &DateTime<Utc>,
        file_id: &str,
    ) -> Result<ObjectPath, DatalakeError> {
        let slug = data_type_slug(data_type)?;
        let code_ok = (1..=64).contains(&error_code.len())
            && error_code
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_');
        if !code_ok {
            return Err(DatalakeError::config(format!(
                "error code {error_code:?} must match [A-Z0-9_]{{1,64}}"
            )));
        }
        let key = format!(
            "{}/tenant_id={tenant}/data_type={slug}/error_code={error_code}/{}/{file_id}.jsonl",
            self.dlq_prefix,
            date_partitions(at)
        );
        Ok(self.object(key))
    }

    /// Prefix holding all data of one tenant and data type (tenant-first layout).
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] when `data_type` is not a valid slug.
    pub fn tenant_prefix(
        &self,
        tenant: &TenantId,
        data_type: &str,
    ) -> Result<String, DatalakeError> {
        let slug = data_type_slug(data_type)?;
        Ok(match self.order {
            PartitionOrder::TenantFirst => {
                format!("{}/tenant_id={tenant}/data_type={slug}/", self.data_prefix)
            }
            PartitionOrder::DataTypeFirst => {
                format!("{}/data_type={slug}/tenant_id={tenant}/", self.data_prefix)
            }
        })
    }

    /// Bucket name.
    #[must_use]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }
    /// Data prefix.
    #[must_use]
    pub fn data_prefix(&self) -> &str {
        &self.data_prefix
    }
    /// Dead-letter prefix.
    #[must_use]
    pub fn dlq_prefix(&self) -> &str {
        &self.dlq_prefix
    }
    /// Partition order.
    #[must_use]
    pub fn partition_order(&self) -> PartitionOrder {
        self.order
    }

    fn object(&self, key: String) -> ObjectPath {
        ObjectPath {
            uri: format!("s3://{}/{key}", self.bucket),
            key,
        }
    }
}

fn date_partitions(at: &DateTime<Utc>) -> String {
    at.format("year=%Y/month=%m/day=%d").to_string()
}

fn ensure_disjoint(data: &str, dlq: &str) -> Result<(), DatalakeError> {
    let overlaps =
        data == dlq || data.starts_with(&format!("{dlq}/")) || dlq.starts_with(&format!("{data}/"));
    if overlaps {
        Err(DatalakeError::config(format!(
            "data prefix {data:?} and dead-letter prefix {dlq:?} must not overlap"
        )))
    } else {
        Ok(())
    }
}

/// Prefixes are one or more `/`-separated segments of `[a-z0-9_-]`, at most
/// 128 characters, with no empty, `.` or `..` segment.
fn validate_prefix(name: &str, prefix: &str) -> Result<(), DatalakeError> {
    let ok = (1..=128).contains(&prefix.len())
        && prefix.split('/').all(|seg| {
            !seg.is_empty()
                && seg
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
        });
    if ok {
        Ok(())
    } else {
        Err(DatalakeError::config(format!(
            "{name} {prefix:?} must be '/'-separated segments of [a-z0-9_-] (max 128 chars)"
        )))
    }
}

/// AWS bucket naming rules, without dots (they break TLS virtual-host addressing).
fn validate_bucket_name(bucket: &str) -> Result<(), DatalakeError> {
    let reason = if !(3..=63).contains(&bucket.len()) {
        Some("must be 3 to 63 characters")
    } else if !bucket
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        Some("only lowercase letters, digits and hyphens are allowed")
    } else if bucket.starts_with('-') || bucket.ends_with('-') {
        Some("cannot start or end with a hyphen")
    } else if bucket.contains("--") {
        Some("cannot contain consecutive hyphens")
    } else if bucket.starts_with("xn--") || bucket.ends_with("-s3alias") {
        Some("uses a reserved prefix or suffix")
    } else {
        None
    };
    match reason {
        Some(r) => Err(DatalakeError::config(format!("bucket {bucket:?}: {r}"))),
        None => Ok(()),
    }
}

/// Normalizes `-` and spaces to `_` and enforces `[a-z0-9_]{1,100}`.
fn data_type_slug(data_type: &str) -> Result<String, DatalakeError> {
    let slug = data_type.to_ascii_lowercase().replace(['-', ' '], "_");
    let ok = (1..=100).contains(&slug.len())
        && slug
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
    if ok {
        Ok(slug)
    } else {
        Err(DatalakeError::config(format!(
            "data type {data_type:?} must normalize to [a-z0-9_]{{1,100}}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn tenant() -> TenantId {
        TenantId::new("my-tenant-001").unwrap()
    }

    fn ts() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 20, 12, 0, 0).unwrap()
    }

    fn resolver() -> BronzePathResolver {
        BronzePathResolver::new("acme-bronze").unwrap()
    }

    #[test]
    fn tenant_first_layout() {
        let p = resolver()
            .data_path(&tenant(), "security-findings", &ts(), "01ULID")
            .unwrap();
        assert_eq!(
            p.key,
            "bronze/tenant_id=my-tenant-001/data_type=security_findings/year=2026/month=09/day=20/01ULID.parquet"
        );
        assert_eq!(p.uri, format!("s3://acme-bronze/{}", p.key));
    }

    #[test]
    fn data_type_first_layout_with_custom_prefix() {
        let r = resolver()
            .with_data_prefix("raw/connectors")
            .unwrap()
            .with_partition_order(PartitionOrder::DataTypeFirst);
        let p = r.data_path(&tenant(), "events", &ts(), "01ULID").unwrap();
        assert_eq!(
            p.key,
            "raw/connectors/data_type=events/tenant_id=my-tenant-001/year=2026/month=09/day=20/01ULID.parquet"
        );
    }

    #[test]
    fn dlq_layout() {
        let r = resolver().with_dlq_prefix("quarantine").unwrap();
        let p = r
            .dlq_path(&tenant(), "events", "ERR_SCHEMA_VIOLATION", &ts(), "01ERR")
            .unwrap();
        assert_eq!(
            p.key,
            "quarantine/tenant_id=my-tenant-001/data_type=events/error_code=ERR_SCHEMA_VIOLATION/year=2026/month=09/day=20/01ERR.jsonl"
        );
    }

    #[test]
    fn dlq_rejects_unsafe_error_codes() {
        for code in ["", "err/../x", "lower", "A=B"] {
            assert!(resolver()
                .dlq_path(&tenant(), "events", code, &ts(), "id")
                .is_err());
        }
    }

    #[test]
    fn tenant_prefix_follows_order() {
        assert_eq!(
            resolver().tenant_prefix(&tenant(), "events").unwrap(),
            "bronze/tenant_id=my-tenant-001/data_type=events/"
        );
    }

    #[test]
    fn rejects_invalid_buckets() {
        let long = "a".repeat(64);
        for b in [
            "ab",
            long.as_str(),
            "-x-y",
            "x-y-",
            "a--b",
            "xn--abc",
            "b-s3alias",
            "My-Bucket",
            "my.bucket",
        ] {
            assert!(BronzePathResolver::new(b).is_err(), "{b:?}");
        }
    }

    #[test]
    fn rejects_unsafe_prefixes() {
        let long = "a".repeat(129);
        for p in [
            "",
            "/bronze",
            "bronze/",
            "a//b",
            "../x",
            "a/./b",
            "Bronze",
            "a b",
            long.as_str(),
        ] {
            assert!(resolver().with_data_prefix(p).is_err(), "{p:?}");
        }
    }

    #[test]
    fn rejects_overlapping_prefixes() {
        assert!(resolver().with_dlq_prefix("bronze").is_err());
        assert!(resolver().with_dlq_prefix("bronze/dlq").is_err());
        assert!(resolver().with_data_prefix("dlq/data").is_err());
        assert!(resolver().with_dlq_prefix("bronze-dlq").is_ok());
    }

    #[test]
    fn rejects_invalid_data_types() {
        let long = "a".repeat(101);
        for d in ["", "a/b", "a.b", "é", long.as_str()] {
            assert!(
                resolver().data_path(&tenant(), d, &ts(), "id").is_err(),
                "{d:?}"
            );
        }
    }

    #[test]
    fn parses_partition_order() {
        assert_eq!(
            PartitionOrder::parse("tenant_first").unwrap(),
            PartitionOrder::TenantFirst
        );
        assert_eq!(
            PartitionOrder::parse("data_type_first").unwrap(),
            PartitionOrder::DataTypeFirst
        );
        assert!(PartitionOrder::parse("other").is_err());
    }
}

#[cfg(test)]
mod proptest_tests {
    use super::*;
    use chrono::TimeZone;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn data_keys_are_hive_and_tenant_scoped(slug in "[a-z][a-z0-9_]{1,30}") {
            let r = BronzePathResolver::new("my-bucket").unwrap();
            let t = TenantId::new("tenant-001").unwrap();
            let at = Utc.with_ymd_and_hms(2026, 6, 15, 0, 0, 0).unwrap();
            let p = r.data_path(&t, &slug, &at, "01ULID").unwrap();
            prop_assert!(p.key.starts_with("bronze/tenant_id=tenant-001/data_type="));
            prop_assert!(p.key.contains("year=2026/month=06/day=15"));
            prop_assert!(p.key.ends_with(".parquet"));
            prop_assert!(!p.key.contains("my-bucket"));
        }

        #[test]
        fn arbitrary_inputs_never_panic(s in ".*") {
            let r = BronzePathResolver::new("my-bucket").unwrap();
            let t = TenantId::new("tenant-001").unwrap();
            let _ = r.data_path(&t, &s, &Utc::now(), "id");
            let _ = r.clone().with_data_prefix(s.clone());
            let _ = BronzePathResolver::new(s);
        }
    }
}
