//! Tenant profiles: what a tenant owns in the lake and how its PII is protected.
//!
//! Micro-tenancy in this SDK means **every call names its tenant, and the
//! tenant decides where its data goes**. A [`TenantProfile`] is an immutable,
//! validated value object that fixes, per tenant:
//!
//! - a [`StorageLocation`] (bucket and key prefix) for each medallion
//!   [`Layer`]: `bronze` and `quarantine` are required because ingestion
//!   writes them; `silver` and `gold` are optional and resolved the same way,
//!   so readers and later layers take their locations from the same profile;
//! - the partition order of data keys;
//! - the PII protection scheme, if any (none by default).
//!
//! Profiles are looked up through the [`TenantDirectory`] port (repository
//! pattern). [`TenantRegistry`] is the in-memory implementation; hosts can
//! back the port with their own configuration store. A profile can also be
//! passed directly on each ingest call.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, Utc};

use super::path::{
    data_key, data_type_slug, dlq_key, validate_bucket_name, validate_prefix, ObjectPath,
    PartitionOrder,
};
use super::TenantId;
use crate::error::DatalakeError;
use crate::validation::pii::{validate_scheme, FieldProtector};

/// A medallion layer a tenant can own a location for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Layer {
    /// Raw records exactly as received and validated (written by ingestion).
    Bronze,
    /// Dead letters of rejected records (written by ingestion).
    Quarantine,
    /// Cleaned and conformed data.
    Silver,
    /// Business-level aggregates.
    Gold,
}

impl Layer {
    /// All layers, in medallion order.
    pub const ALL: [Self; 4] = [Self::Bronze, Self::Quarantine, Self::Silver, Self::Gold];

    /// Lowercase name, also the default key prefix.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bronze => "bronze",
            Self::Quarantine => "quarantine",
            Self::Silver => "silver",
            Self::Gold => "gold",
        }
    }

    /// Parses a layer name.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] for an unknown name.
    pub fn parse(name: &str) -> Result<Self, DatalakeError> {
        Self::ALL
            .into_iter()
            .find(|l| l.as_str() == name)
            .ok_or_else(|| {
                DatalakeError::config(format!(
                    "layer must be bronze, quarantine, silver or gold, got {name:?}"
                ))
            })
    }
}

/// A bucket and key prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageLocation {
    bucket: String,
    prefix: String,
}

impl StorageLocation {
    /// Creates a location.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] for an invalid bucket name or an
    /// unsafe prefix.
    pub fn new(
        bucket: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Result<Self, DatalakeError> {
        let (bucket, prefix) = (bucket.into(), prefix.into());
        validate_bucket_name(&bucket)?;
        validate_prefix("prefix", &prefix)?;
        Ok(Self { bucket, prefix })
    }

    /// Parses `s3://bucket/prefix`.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] when the URI is not of that form or
    /// its parts are invalid.
    pub fn parse(uri: &str) -> Result<Self, DatalakeError> {
        let rest = uri.strip_prefix("s3://").ok_or_else(|| {
            DatalakeError::config(format!("location {uri:?} must start with s3://"))
        })?;
        let (bucket, prefix) = rest.split_once('/').ok_or_else(|| {
            DatalakeError::config(format!(
                "location {uri:?} must be s3://bucket/prefix (a prefix is required)"
            ))
        })?;
        Self::new(bucket, prefix.trim_end_matches('/'))
    }

    /// Bucket name.
    #[must_use]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Key prefix, without leading or trailing `/`.
    #[must_use]
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// `s3://bucket/prefix`.
    #[must_use]
    pub fn uri(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.prefix)
    }

    /// Whether one location contains the other.
    fn overlaps(&self, other: &Self) -> bool {
        self.bucket == other.bucket
            && (self.prefix == other.prefix
                || self.prefix.starts_with(&format!("{}/", other.prefix))
                || other.prefix.starts_with(&format!("{}/", self.prefix)))
    }
}

/// Everything the SDK needs to know about one tenant. Immutable once built.
pub struct TenantProfile {
    tenant_id: TenantId,
    locations: BTreeMap<Layer, StorageLocation>,
    partition_order: PartitionOrder,
    protector: Option<Arc<dyn FieldProtector>>,
}

impl fmt::Debug for TenantProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let locations: BTreeMap<&str, String> = self
            .locations
            .iter()
            .map(|(l, loc)| (l.as_str(), loc.uri()))
            .collect();
        f.debug_struct("TenantProfile")
            .field("tenant_id", &self.tenant_id.as_str())
            .field("locations", &locations)
            .field("partition_order", &self.partition_order)
            .field("pii_protection", &self.pii_scheme())
            .finish_non_exhaustive()
    }
}

impl TenantProfile {
    /// Starts a profile for `tenant_id`.
    #[must_use]
    pub fn builder(tenant_id: TenantId) -> TenantProfileBuilder {
        TenantProfileBuilder {
            tenant_id,
            locations: BTreeMap::new(),
            partition_order: PartitionOrder::default(),
            protector: None,
        }
    }

    /// Profile with every layer in one bucket under the default prefixes
    /// `bronze/`, `quarantine/`, `silver/` and `gold/`.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] for an invalid bucket name.
    pub fn for_bucket(
        tenant_id: TenantId,
        bucket: &str,
    ) -> Result<TenantProfileBuilder, DatalakeError> {
        let mut builder = Self::builder(tenant_id);
        for layer in Layer::ALL {
            builder = builder.location(layer, StorageLocation::new(bucket, layer.as_str())?);
        }
        Ok(builder)
    }

    /// The tenant.
    #[must_use]
    pub fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Location of `layer`, if the profile defines one.
    #[must_use]
    pub fn location(&self, layer: Layer) -> Option<&StorageLocation> {
        self.locations.get(&layer)
    }

    /// Partition order of data keys.
    #[must_use]
    pub fn partition_order(&self) -> PartitionOrder {
        self.partition_order
    }

    /// PII protector, `None` when PII is stored as received.
    #[must_use]
    pub fn protector(&self) -> Option<&dyn FieldProtector> {
        self.protector.as_deref()
    }

    /// Scheme name of the PII protector, or `none`.
    #[must_use]
    pub fn pii_scheme(&self) -> &str {
        self.protector
            .as_deref()
            .map_or("none", FieldProtector::scheme)
    }

    fn required(&self, layer: Layer) -> &StorageLocation {
        // `build` guarantees bronze and quarantine.
        &self.locations[&layer]
    }

    /// Location of a Bronze Parquet object.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] when `data_type` is not a valid slug.
    pub fn data_path(
        &self,
        data_type: &str,
        at: &DateTime<Utc>,
        file_id: &str,
    ) -> Result<ObjectPath, DatalakeError> {
        let loc = self.required(Layer::Bronze);
        let key = data_key(
            &loc.prefix,
            self.partition_order,
            &self.tenant_id,
            data_type,
            at,
            file_id,
        )?;
        Ok(ObjectPath::new(&loc.bucket, key))
    }

    /// Location of a dead-letter object.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] when `data_type` or `error_code` is invalid.
    pub fn dlq_path(
        &self,
        data_type: &str,
        error_code: &str,
        at: &DateTime<Utc>,
        file_id: &str,
    ) -> Result<ObjectPath, DatalakeError> {
        let loc = self.required(Layer::Quarantine);
        let key = dlq_key(
            &loc.prefix,
            &self.tenant_id,
            data_type,
            error_code,
            at,
            file_id,
        )?;
        Ok(ObjectPath::new(&loc.bucket, key))
    }

    /// `s3://` prefix holding this tenant's objects of `data_type` in
    /// `layer`, laid out like Bronze data. `None` when the layer is not defined.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] when `data_type` is not a valid slug.
    pub fn table_uri(
        &self,
        layer: Layer,
        data_type: &str,
    ) -> Result<Option<String>, DatalakeError> {
        let slug = data_type_slug(data_type)?;
        let tenant = &self.tenant_id;
        Ok(self
            .locations
            .get(&layer)
            .map(|loc| match self.partition_order {
                PartitionOrder::TenantFirst => {
                    format!("{}/tenant_id={tenant}/data_type={slug}/", loc.uri())
                }
                PartitionOrder::DataTypeFirst => {
                    format!("{}/data_type={slug}/tenant_id={tenant}/", loc.uri())
                }
            }))
    }
}

/// Builder of a [`TenantProfile`]; [`build`](Self::build) validates it.
pub struct TenantProfileBuilder {
    tenant_id: TenantId,
    locations: BTreeMap<Layer, StorageLocation>,
    partition_order: PartitionOrder,
    protector: Option<Arc<dyn FieldProtector>>,
}

impl TenantProfileBuilder {
    /// Sets the location of `layer`, replacing any previous one.
    #[must_use]
    pub fn location(mut self, layer: Layer, location: StorageLocation) -> Self {
        self.locations.insert(layer, location);
        self
    }

    /// Removes the location of `layer`.
    #[must_use]
    pub fn without(mut self, layer: Layer) -> Self {
        self.locations.remove(&layer);
        self
    }

    /// Sets the partition order of data keys.
    #[must_use]
    pub fn partition_order(mut self, order: PartitionOrder) -> Self {
        self.partition_order = order;
        self
    }

    /// Sets the PII protector; `None` stores PII as received (the default).
    #[must_use]
    pub fn pii_protection(mut self, protector: Option<Arc<dyn FieldProtector>>) -> Self {
        self.protector = protector;
        self
    }

    /// Validates and builds the profile.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] when `bronze` or `quarantine` is
    /// missing, two layers share or nest the same location (layers must stay
    /// separate), or the protector's scheme name is invalid.
    pub fn build(self) -> Result<TenantProfile, DatalakeError> {
        for layer in [Layer::Bronze, Layer::Quarantine] {
            if !self.locations.contains_key(&layer) {
                return Err(DatalakeError::config(format!(
                    "tenant '{}' needs a {} location",
                    self.tenant_id,
                    layer.as_str()
                )));
            }
        }
        let all: Vec<(&Layer, &StorageLocation)> = self.locations.iter().collect();
        for (i, (a, la)) in all.iter().enumerate() {
            for (b, lb) in &all[i + 1..] {
                if la.overlaps(lb) {
                    return Err(DatalakeError::config(format!(
                        "tenant '{}': {} location {} and {} location {} overlap; layers must not share a prefix",
                        self.tenant_id,
                        a.as_str(),
                        la.uri(),
                        b.as_str(),
                        lb.uri()
                    )));
                }
            }
        }
        if let Some(p) = &self.protector {
            validate_scheme(p.scheme())?;
        }
        Ok(TenantProfile {
            tenant_id: self.tenant_id,
            locations: self.locations,
            partition_order: self.partition_order,
            protector: self.protector,
        })
    }
}

/// Port for looking up tenant profiles (repository pattern).
pub trait TenantDirectory: Send + Sync {
    /// Profile of `tenant_id`.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::TenantViolation`] for an invalid or unknown tenant.
    fn resolve(&self, tenant_id: &str) -> Result<Arc<TenantProfile>, DatalakeError>;
}

/// Immutable in-memory [`TenantDirectory`], built once at startup.
#[derive(Debug, Clone, Default)]
pub struct TenantRegistry {
    profiles: BTreeMap<String, Arc<TenantProfile>>,
}

impl TenantRegistry {
    /// Builds a registry.
    ///
    /// # Errors
    ///
    /// [`DatalakeError::Configuration`] when two profiles share a tenant id.
    pub fn new(
        profiles: impl IntoIterator<Item = Arc<TenantProfile>>,
    ) -> Result<Self, DatalakeError> {
        let mut map = BTreeMap::new();
        for profile in profiles {
            let id = profile.tenant_id().as_str().to_owned();
            if map.insert(id.clone(), profile).is_some() {
                return Err(DatalakeError::config(format!(
                    "tenant '{id}' is registered twice"
                )));
            }
        }
        Ok(Self { profiles: map })
    }

    /// Registered tenant ids, sorted.
    pub fn tenant_ids(&self) -> impl Iterator<Item = &str> {
        self.profiles.keys().map(String::as_str)
    }

    /// Number of tenants.
    #[must_use]
    pub fn len(&self) -> usize {
        self.profiles.len()
    }

    /// Whether the registry is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.profiles.is_empty()
    }
}

impl TenantDirectory for TenantRegistry {
    fn resolve(&self, tenant_id: &str) -> Result<Arc<TenantProfile>, DatalakeError> {
        let id = TenantId::new(tenant_id)?;
        self.profiles
            .get(id.as_str())
            .cloned()
            .ok_or_else(|| DatalakeError::TenantViolation {
                tenant_id: tenant_id.to_owned(),
                reason: "no profile is registered for this tenant".into(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validation::pii::{HmacSha256Hasher, Sha256Hasher};
    use chrono::TimeZone;

    fn tenant(id: &str) -> TenantId {
        TenantId::new(id).unwrap()
    }

    fn at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 17, 8, 0, 0).unwrap()
    }

    #[test]
    fn for_bucket_defines_every_layer_and_no_protection() {
        let p = TenantProfile::for_bucket(tenant("acme-001"), "acme-lake")
            .unwrap()
            .build()
            .unwrap();
        for layer in Layer::ALL {
            assert_eq!(
                p.location(layer).unwrap().uri(),
                format!("s3://acme-lake/{}", layer.as_str())
            );
        }
        assert_eq!(p.pii_scheme(), "none");
        assert!(p.protector().is_none());
        let d = p.data_path("login_event", &at(), "01X").unwrap();
        assert_eq!(d.bucket, "acme-lake");
        assert_eq!(
            d.key,
            "bronze/tenant_id=acme-001/data_type=login_event/year=2026/month=10/day=17/01X.parquet"
        );
        let q = p.dlq_path("login_event", "ERR_X", &at(), "01X").unwrap();
        assert!(q
            .uri
            .starts_with("s3://acme-lake/quarantine/tenant_id=acme-001/"));
        assert_eq!(
            p.table_uri(Layer::Silver, "login_event").unwrap().unwrap(),
            "s3://acme-lake/silver/tenant_id=acme-001/data_type=login_event/"
        );
    }

    #[test]
    fn layers_can_live_in_dedicated_buckets() {
        let p = TenantProfile::builder(tenant("acme-001"))
            .location(
                Layer::Bronze,
                StorageLocation::parse("s3://acme-raw/landing/bronze").unwrap(),
            )
            .location(
                Layer::Quarantine,
                StorageLocation::parse("s3://acme-quarantine/dlq/").unwrap(),
            )
            .partition_order(PartitionOrder::DataTypeFirst)
            .pii_protection(Some(Arc::new(Sha256Hasher)))
            .build()
            .unwrap();
        assert!(p.location(Layer::Gold).is_none());
        assert_eq!(p.table_uri(Layer::Gold, "x").unwrap(), None);
        assert_eq!(p.pii_scheme(), "sha256");
        let d = p.data_path("x", &at(), "1").unwrap();
        assert_eq!(
            d.uri.split('/').take(6).collect::<Vec<_>>().join("/"),
            "s3://acme-raw/landing/bronze/data_type=x"
        );
        assert_eq!(
            p.dlq_path("x", "E", &at(), "1").unwrap().bucket,
            "acme-quarantine"
        );
    }

    #[test]
    fn required_layers_and_separation_are_enforced() {
        let only_bronze = TenantProfile::builder(tenant("tenant-01"))
            .location(
                Layer::Bronze,
                StorageLocation::new("lake-a", "bronze").unwrap(),
            )
            .build();
        assert!(only_bronze.is_err());
        for (a, b) in [
            ("bronze", "bronze"),
            ("data", "data/dlq"),
            ("data/x", "data"),
        ] {
            let r = TenantProfile::builder(tenant("tenant-01"))
                .location(Layer::Bronze, StorageLocation::new("lake-a", a).unwrap())
                .location(
                    Layer::Quarantine,
                    StorageLocation::new("lake-a", b).unwrap(),
                )
                .build();
            assert!(r.is_err(), "{a} vs {b}");
        }
        let other_bucket = TenantProfile::builder(tenant("tenant-01"))
            .location(
                Layer::Bronze,
                StorageLocation::new("lake-a", "data").unwrap(),
            )
            .location(
                Layer::Quarantine,
                StorageLocation::new("lake-b", "data").unwrap(),
            )
            .build();
        assert!(other_bucket.is_ok());
        let nested_gold = TenantProfile::for_bucket(tenant("tenant-01"), "lake-a")
            .unwrap()
            .location(
                Layer::Gold,
                StorageLocation::new("lake-a", "silver/gold").unwrap(),
            )
            .build();
        assert!(nested_gold.is_err());
    }

    #[test]
    fn locations_are_validated() {
        for bad in [
            "acme/bronze",
            "s3://acme",
            "s3://Acme/x",
            "s3://acme-lake/../x",
            "s3://acme-lake/",
        ] {
            assert!(StorageLocation::parse(bad).is_err(), "{bad}");
        }
        assert!(Layer::parse("platinum").is_err());
        assert_eq!(Layer::parse("silver").unwrap(), Layer::Silver);
    }

    #[test]
    fn registry_resolves_known_tenants_only() {
        let a = Arc::new(
            TenantProfile::for_bucket(tenant("acme-001"), "lake-a")
                .unwrap()
                .build()
                .unwrap(),
        );
        let b = Arc::new(
            TenantProfile::for_bucket(tenant("beta-002"), "lake-b")
                .unwrap()
                .pii_protection(Some(Arc::new(HmacSha256Hasher::new(&[1; 32]).unwrap())))
                .build()
                .unwrap(),
        );
        let reg = TenantRegistry::new([Arc::clone(&a), b]).unwrap();
        assert_eq!(
            reg.tenant_ids().collect::<Vec<_>>(),
            ["acme-001", "beta-002"]
        );
        assert_eq!(reg.resolve("beta-002").unwrap().pii_scheme(), "hmac-sha256");
        assert!(matches!(
            reg.resolve("ghost-003"),
            Err(DatalakeError::TenantViolation { .. })
        ));
        assert!(matches!(
            reg.resolve("../x"),
            Err(DatalakeError::TenantViolation { .. })
        ));
        assert!(TenantRegistry::new([Arc::clone(&a), a]).is_err());
    }

    #[test]
    fn debug_shows_layout_but_no_keys() {
        let p = TenantProfile::for_bucket(tenant("acme-001"), "acme-lake")
            .unwrap()
            .pii_protection(Some(Arc::new(
                HmacSha256Hasher::new(b"0123456789abcdef-secret").unwrap(),
            )))
            .build()
            .unwrap();
        let shown = format!("{p:?}");
        assert!(shown.contains("s3://acme-lake/bronze") && shown.contains("hmac-sha256"));
        assert!(!shown.contains("secret"));
    }
}
