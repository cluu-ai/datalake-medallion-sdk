//! Hive-partitioned object keys and the validation rules for locations.
//!
//! Every key keeps the tenant in a `tenant_id=` partition, even when a tenant
//! has buckets of its own, so isolation never depends on bucket layout alone:
//!
//! ```text
//! TenantFirst:   <bronze prefix>/tenant_id=<t>/data_type=<d>/year=YYYY/month=MM/day=DD/<id>.parquet
//! DataTypeFirst: <bronze prefix>/data_type=<d>/tenant_id=<t>/year=YYYY/month=MM/day=DD/<id>.parquet
//! Quarantine:    <quarantine prefix>/tenant_id=<t>/data_type=<d>/error_code=<c>/year=YYYY/month=MM/day=DD/<id>.jsonl
//! ```

use chrono::{DateTime, Utc};

use crate::error::DatalakeError;
use crate::tenant::TenantId;

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

    /// Keyword value of this order.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TenantFirst => "tenant_first",
            Self::DataTypeFirst => "data_type_first",
        }
    }
}

/// A resolved object location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectPath {
    /// Bucket name.
    pub bucket: String,
    /// Key inside the bucket.
    pub key: String,
    /// `s3://bucket/key`.
    pub uri: String,
}

impl ObjectPath {
    pub(crate) fn new(bucket: &str, key: String) -> Self {
        Self {
            uri: format!("s3://{bucket}/{key}"),
            bucket: bucket.to_owned(),
            key,
        }
    }
}

/// Key of a Parquet data object below `prefix`.
pub(crate) fn data_key(
    prefix: &str,
    order: PartitionOrder,
    tenant: &TenantId,
    data_type: &str,
    at: &DateTime<Utc>,
    file_id: &str,
) -> Result<String, DatalakeError> {
    let slug = data_type_slug(data_type)?;
    let (first, second) = match order {
        PartitionOrder::TenantFirst => (format!("tenant_id={tenant}"), format!("data_type={slug}")),
        PartitionOrder::DataTypeFirst => {
            (format!("data_type={slug}"), format!("tenant_id={tenant}"))
        }
    };
    Ok(format!(
        "{prefix}/{first}/{second}/{}/{file_id}.parquet",
        date_partitions(at)
    ))
}

/// Key of a newline-delimited JSON dead-letter object below `prefix`.
pub(crate) fn dlq_key(
    prefix: &str,
    tenant: &TenantId,
    data_type: &str,
    error_code: &str,
    at: &DateTime<Utc>,
    file_id: &str,
) -> Result<String, DatalakeError> {
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
    Ok(format!(
        "{prefix}/tenant_id={tenant}/data_type={slug}/error_code={error_code}/{}/{file_id}.jsonl",
        date_partitions(at)
    ))
}

/// Prefix of a table's commit manifests below the Bronze `prefix`. The
/// leading underscore keeps engines that scan the Bronze tree (Hive, Spark,
/// Athena) from reading it as data.
pub(crate) fn manifest_prefix(
    prefix: &str,
    tenant: &TenantId,
    data_type: &str,
) -> Result<String, DatalakeError> {
    let slug = data_type_slug(data_type)?;
    Ok(format!(
        "{prefix}/_manifests/tenant_id={tenant}/data_type={slug}"
    ))
}

/// Key of one manifest entry: partitioned by commit day, named so keys sort
/// by commit time.
pub(crate) fn manifest_key(
    prefix: &str,
    tenant: &TenantId,
    data_type: &str,
    at: &DateTime<Utc>,
    file_id: &str,
) -> Result<String, DatalakeError> {
    Ok(format!(
        "{}/date={}/{file_id}.json",
        manifest_prefix(prefix, tenant, data_type)?,
        at.format("%Y-%m-%d")
    ))
}

fn date_partitions(at: &DateTime<Utc>) -> String {
    at.format("year=%Y/month=%m/day=%d").to_string()
}

/// Prefixes are one or more `/`-separated segments of `[a-z0-9_-]`, at most
/// 128 characters, with no empty, `.` or `..` segment.
pub(crate) fn validate_prefix(name: &str, prefix: &str) -> Result<(), DatalakeError> {
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
pub(crate) fn validate_bucket_name(bucket: &str) -> Result<(), DatalakeError> {
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
pub(crate) fn data_type_slug(data_type: &str) -> Result<String, DatalakeError> {
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
        TenantId::new("acme-001").unwrap()
    }

    fn ts() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 3, 7, 12, 0, 0).unwrap()
    }

    #[test]
    fn data_keys_follow_partition_order() {
        let first = data_key(
            "bronze",
            PartitionOrder::TenantFirst,
            &tenant(),
            "Login-Event",
            &ts(),
            "01X",
        )
        .unwrap();
        assert_eq!(
            first,
            "bronze/tenant_id=acme-001/data_type=login_event/year=2026/month=03/day=07/01X.parquet"
        );
        let by_type = data_key(
            "raw/b",
            PartitionOrder::DataTypeFirst,
            &tenant(),
            "x",
            &ts(),
            "01X",
        )
        .unwrap();
        assert!(by_type.starts_with("raw/b/data_type=x/tenant_id=acme-001/"));
    }

    #[test]
    fn dlq_keys_and_unsafe_codes() {
        let k = dlq_key("quarantine", &tenant(), "t", "ERR_X", &ts(), "01X").unwrap();
        assert_eq!(
            k,
            "quarantine/tenant_id=acme-001/data_type=t/error_code=ERR_X/year=2026/month=03/day=07/01X.jsonl"
        );
        for bad in ["", "err_x", "ERR/..", &"E".repeat(65)] {
            assert!(
                dlq_key("q", &tenant(), "t", bad, &ts(), "id").is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn object_paths_carry_bucket_and_uri() {
        let p = ObjectPath::new("acme-lake", "a/b.parquet".into());
        assert_eq!(p.uri, "s3://acme-lake/a/b.parquet");
        assert_eq!(p.bucket, "acme-lake");
    }

    #[test]
    fn rejects_invalid_buckets() {
        for bad in [
            "ab",
            "Upper",
            "has.dot",
            "-start",
            "end-",
            "a--b",
            "xn--x",
            "b-s3alias",
            &"a".repeat(64),
        ] {
            assert!(validate_bucket_name(bad).is_err(), "{bad}");
        }
        assert!(validate_bucket_name("acme-lake-01").is_ok());
    }

    #[test]
    fn rejects_unsafe_prefixes() {
        for bad in [
            "",
            "/a",
            "a/",
            "a//b",
            "../x",
            "a/./b",
            "UP",
            "a b",
            &"a".repeat(129),
        ] {
            assert!(validate_prefix("p", bad).is_err(), "{bad}");
        }
        assert!(validate_prefix("p", "raw/bronze-v1").is_ok());
    }

    #[test]
    fn rejects_invalid_data_types() {
        for bad in ["", "a/b", "../x", "ñ", &"a".repeat(101)] {
            assert!(data_type_slug(bad).is_err(), "{bad}");
        }
        assert_eq!(data_type_slug("Login Event").unwrap(), "login_event");
    }

    #[test]
    fn parses_partition_order() {
        for order in [PartitionOrder::TenantFirst, PartitionOrder::DataTypeFirst] {
            assert_eq!(PartitionOrder::parse(order.as_str()).unwrap(), order);
        }
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
            let t = TenantId::new("tenant-001").unwrap();
            let at = Utc.with_ymd_and_hms(2026, 6, 15, 0, 0, 0).unwrap();
            let k = data_key("bronze", PartitionOrder::TenantFirst, &t, &slug, &at, "01ULID").unwrap();
            prop_assert!(k.starts_with("bronze/tenant_id=tenant-001/data_type="));
            prop_assert!(k.contains("year=2026/month=06/day=15"));
            prop_assert!(k.ends_with(".parquet"));
        }

        #[test]
        fn arbitrary_inputs_never_panic(s in ".*") {
            let t = TenantId::new("tenant-001").unwrap();
            let _ = data_key("bronze", PartitionOrder::TenantFirst, &t, &s, &Utc::now(), "id");
            let _ = validate_prefix("p", &s);
            let _ = validate_bucket_name(&s);
        }
    }
}
