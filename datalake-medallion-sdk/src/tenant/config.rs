//! Declarative tenant configuration for [`TenantRegistry::from_config`].
//!
//! ```json
//! {"tenants": [{
//!   "tenant_id": "acme-001",
//!   "bronze": "s3://acme-lake/bronze", "quarantine": "s3://acme-lake/quarantine",
//!   "silver": "s3://acme-lake/silver",
//!   "partition_order": "tenant_first",
//!   "pii_protection": {"scheme": "aes-256-gcm", "provider": "aws-kms",
//!                      "kms_key_id": "alias/lake-acme", "data_key_max_age_seconds": 3600}
//! }]}
//! ```
//!
//! Unknown fields are rejected, so a typo cannot silently disable protection.
//! Secrets never live in the document: HMAC keys are read from the
//! environment variable named by `key_env`, and KMS keys stay in KMS.
//! `pii_protection` is optional; without it PII is stored as received.

use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use serde_json::Value;

use super::{Layer, PartitionOrder, StorageLocation, TenantId, TenantProfile, TenantRegistry};
use crate::error::DatalakeError;
use crate::validation::keys::{EnvelopeProtector, KeyCachePolicy};
use crate::validation::pii::{FieldProtector, HmacSha256Hasher, Sha256Hasher};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistryConfig {
    tenants: Vec<TenantConfig>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TenantConfig {
    tenant_id: String,
    bronze: String,
    quarantine: String,
    silver: Option<String>,
    gold: Option<String>,
    partition_order: Option<String>,
    pii_protection: Option<ProtectionConfig>,
}

#[derive(Deserialize)]
#[serde(tag = "scheme", deny_unknown_fields)]
enum ProtectionConfig {
    // Empty struct variants (not unit variants): serde only rejects unknown
    // fields of internally tagged variants that have a body.
    #[serde(rename = "none")]
    None {},
    #[serde(rename = "sha256")]
    Sha256 {},
    #[serde(rename = "hmac-sha256")]
    HmacSha256 { key_env: String },
    #[serde(rename = "aes-256-gcm")]
    Aes256Gcm {
        provider: String,
        kms_key_id: String,
        data_key_max_age_seconds: Option<u64>,
        max_nonce_prefixes_per_key: Option<u64>,
        region: Option<String>,
        endpoint_url: Option<String>,
    },
}

fn protector(
    config: ProtectionConfig,
    tenant: &str,
) -> Result<Option<Arc<dyn FieldProtector>>, DatalakeError> {
    Ok(match config {
        ProtectionConfig::None {} => None,
        ProtectionConfig::Sha256 {} => Some(Arc::new(Sha256Hasher)),
        ProtectionConfig::HmacSha256 { key_env } => {
            let key = std::env::var(&key_env).map_err(|_| {
                DatalakeError::config(format!(
                    "tenant '{tenant}': environment variable {key_env} with the HMAC key is not set"
                ))
            })?;
            Some(Arc::new(HmacSha256Hasher::new(key.as_bytes())?))
        }
        ProtectionConfig::Aes256Gcm {
            provider,
            kms_key_id,
            data_key_max_age_seconds,
            max_nonce_prefixes_per_key,
            region,
            endpoint_url,
        } => {
            let defaults = KeyCachePolicy::default();
            let policy = KeyCachePolicy {
                max_age: data_key_max_age_seconds.map_or(defaults.max_age, Duration::from_secs),
                max_nonces: max_nonce_prefixes_per_key.unwrap_or(defaults.max_nonces),
                ..defaults
            };
            let provider = data_key_provider(&provider, kms_key_id, region, endpoint_url)?;
            Some(Arc::new(EnvelopeProtector::new(provider, policy)?))
        }
    })
}

#[cfg(feature = "aws-kms")]
fn data_key_provider(
    provider: &str,
    kms_key_id: String,
    region: Option<String>,
    endpoint_url: Option<String>,
) -> Result<Arc<dyn crate::validation::keys::DataKeyProvider>, DatalakeError> {
    use crate::validation::aws_kms::{AwsKmsDataKeyProvider, KmsOptions};
    match provider {
        "aws-kms" => Ok(Arc::new(AwsKmsDataKeyProvider::new(
            kms_key_id,
            KmsOptions {
                region,
                endpoint_url,
            },
        )?)),
        other => Err(DatalakeError::config(format!(
            "unknown data key provider {other:?}; supported: aws-kms (others through code)"
        ))),
    }
}

#[cfg(not(feature = "aws-kms"))]
fn data_key_provider(
    provider: &str,
    _: String,
    _: Option<String>,
    _: Option<String>,
) -> Result<Arc<dyn crate::validation::keys::DataKeyProvider>, DatalakeError> {
    Err(DatalakeError::config(format!(
        "data key provider {provider:?} is not available: built without the aws-kms feature"
    )))
}

impl TenantRegistry {
    /// Builds a registry from a configuration document (see the module docs).
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] for an invalid document, a duplicated
    /// tenant, a missing HMAC key variable or an unknown provider; profile
    /// validation errors as in [`TenantProfile`].
    pub fn from_config(config: &Value) -> Result<Self, DatalakeError> {
        let parsed: RegistryConfig = serde_json::from_value(config.clone())
            .map_err(|e| DatalakeError::config(format!("invalid tenant configuration: {e}")))?;
        let mut profiles = Vec::with_capacity(parsed.tenants.len());
        for t in parsed.tenants {
            let mut builder = TenantProfile::builder(TenantId::new(&t.tenant_id)?)
                .location(Layer::Bronze, StorageLocation::parse(&t.bronze)?)
                .location(Layer::Quarantine, StorageLocation::parse(&t.quarantine)?);
            for (layer, uri) in [(Layer::Silver, t.silver), (Layer::Gold, t.gold)] {
                if let Some(uri) = uri {
                    builder = builder.location(layer, StorageLocation::parse(&uri)?);
                }
            }
            if let Some(order) = t.partition_order {
                builder = builder.partition_order(PartitionOrder::parse(&order)?);
            }
            let protection = match t.pii_protection {
                Some(p) => protector(p, &t.tenant_id)?,
                None => None,
            };
            profiles.push(Arc::new(builder.pii_protection(protection).build()?));
        }
        Self::new(profiles)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant::TenantDirectory;
    use serde_json::json;

    #[test]
    fn builds_profiles_with_every_scheme() {
        std::env::set_var("DL_TEST_HMAC_KEY", "k".repeat(32));
        let reg = TenantRegistry::from_config(&json!({"tenants": [
            {"tenant_id": "plain-001", "bronze": "s3://lake-a/bronze", "quarantine": "s3://lake-a/quarantine"},
            {"tenant_id": "hash-002", "bronze": "s3://lake-b/bronze", "quarantine": "s3://lake-b/q",
             "silver": "s3://lake-b/silver", "partition_order": "data_type_first",
             "pii_protection": {"scheme": "sha256"}},
            {"tenant_id": "hmac-003", "bronze": "s3://lake-c/b", "quarantine": "s3://lake-c/q",
             "pii_protection": {"scheme": "hmac-sha256", "key_env": "DL_TEST_HMAC_KEY"}},
            {"tenant_id": "none-004", "bronze": "s3://lake-d/b", "quarantine": "s3://lake-d/q",
             "pii_protection": {"scheme": "none"}}
        ]}))
        .unwrap();
        assert_eq!(reg.resolve("plain-001").unwrap().pii_scheme(), "none");
        let hashed = reg.resolve("hash-002").unwrap();
        assert_eq!(hashed.pii_scheme(), "sha256");
        assert_eq!(hashed.partition_order(), PartitionOrder::DataTypeFirst);
        assert!(hashed.location(Layer::Silver).is_some());
        assert_eq!(reg.resolve("hmac-003").unwrap().pii_scheme(), "hmac-sha256");
        assert_eq!(reg.resolve("none-004").unwrap().pii_scheme(), "none");
    }

    #[cfg(feature = "aws-kms")]
    #[test]
    fn kms_envelope_from_config_needs_no_network_until_used() {
        let reg = TenantRegistry::from_config(&json!({"tenants": [
            {"tenant_id": "kms-001", "bronze": "s3://lake-a/b", "quarantine": "s3://lake-a/q",
             "pii_protection": {"scheme": "aes-256-gcm", "provider": "aws-kms",
                                "kms_key_id": "alias/lake-kms-001", "data_key_max_age_seconds": 900}}
        ]}))
        .unwrap();
        assert_eq!(reg.resolve("kms-001").unwrap().pii_scheme(), "aes-256-gcm");
    }

    #[test]
    fn rejects_typos_missing_keys_and_bad_values() {
        let base = |p: Value| {
            json!({"tenants": [{"tenant_id": "acme-001", "bronze": "s3://lake-a/b",
                                                  "quarantine": "s3://lake-a/q", "pii_protection": p}]})
        };
        for bad in [
            json!({"tenants": [{"tenant_id": "acme-001", "bronze": "s3://lake-a/b", "quarantine": "s3://lake-a/q", "pii_protecton": {}}]}),
            json!({"tenant": []}),
            base(json!({"scheme": "rot13"})),
            base(json!({"scheme": "sha256", "salt": "x"})),
            base(json!({"scheme": "hmac-sha256", "key_env": "DL_TEST_MISSING_VARIABLE"})),
            base(json!({"scheme": "aes-256-gcm", "provider": "vault", "kms_key_id": "k"})),
            base(
                json!({"scheme": "aes-256-gcm", "provider": "aws-kms", "kms_key_id": "k", "data_key_max_age_seconds": 5}),
            ),
            json!({"tenants": [{"tenant_id": "acme-001", "bronze": "s3://lake-a/b", "quarantine": "s3://lake-a/b"}]}),
        ] {
            assert!(TenantRegistry::from_config(&bad).is_err(), "{bad}");
        }
    }
}
