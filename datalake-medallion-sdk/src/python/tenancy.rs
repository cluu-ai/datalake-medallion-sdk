//! Python classes for tenant profiles and PII protection schemes.

use std::fmt;
use std::sync::Arc;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::error::DatalakeError;
use crate::tenant::{
    Layer, PartitionOrder, StorageLocation, TenantDirectory, TenantId, TenantProfile,
    TenantRegistry,
};
use crate::validation::pii::{
    validate_scheme, Aes256GcmEncryptor, FieldProtector, HmacSha256Hasher, ProtectionContext,
    Sha256Hasher,
};

/// Adapts a Python object to [`FieldProtector`]: it needs a `scheme` string,
/// an optional `key_ref` and `protect_batch(tenant_id, field, values)`
/// returning one string per value. Called once per field and batch, with the
/// GIL held only during the call. Exceptions are reported by type name only.
struct PyFieldProtector {
    object: Py<PyAny>,
    scheme: String,
    key_ref: Option<String>,
}

impl fmt::Debug for PyFieldProtector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PyFieldProtector({})", self.scheme)
    }
}

impl FieldProtector for PyFieldProtector {
    fn scheme(&self) -> &str {
        &self.scheme
    }

    fn key_ref(&self) -> Option<&str> {
        self.key_ref.as_deref()
    }

    fn protect(&self, ctx: &ProtectionContext<'_>, value: &str) -> Result<String, DatalakeError> {
        self.protect_batch(ctx, &[value])?
            .pop()
            .ok_or_else(|| DatalakeError::protection("custom protector returned no value"))
    }

    fn protect_batch(
        &self,
        ctx: &ProtectionContext<'_>,
        values: &[&str],
    ) -> Result<Vec<String>, DatalakeError> {
        Python::try_attach(|py| {
            let call = || -> PyResult<Vec<String>> {
                let list = PyList::new(py, values)?;
                self.object
                    .bind(py)
                    .call_method1("protect_batch", (ctx.tenant_id, ctx.field, list))?
                    .extract()
            };
            call().map_err(|err| {
                let name = err
                    .get_type(py)
                    .name()
                    .map_or_else(|_| "exception".to_owned(), |n| n.to_string());
                DatalakeError::protection(format!(
                    "protector '{}' raised {name} for field '{}'",
                    self.scheme, ctx.field
                ))
            })
        })
        .unwrap_or_else(|| Err(DatalakeError::protection("Python interpreter unavailable")))
    }
}

/// A PII protection scheme for a tenant profile.
#[pyclass(
    name = "PiiProtection",
    module = "datalake_medallion_sdk._core",
    frozen
)]
pub(crate) struct PyPiiProtection {
    inner: Arc<dyn FieldProtector>,
    aes: Option<Arc<Aes256GcmEncryptor>>,
}

#[pymethods]
impl PyPiiProtection {
    /// Unkeyed SHA-256 (hex). Deterministic; weak for low-entropy values.
    #[staticmethod]
    fn sha256() -> Self {
        Self {
            inner: Arc::new(Sha256Hasher),
            aes: None,
        }
    }

    /// Keyed HMAC-SHA256 (hex) with a key of 16+ bytes kept outside the lake.
    #[staticmethod]
    fn hmac_sha256(key: Vec<u8>) -> PyResult<Self> {
        Ok(Self {
            inner: Arc::new(HmacSha256Hasher::new(&key)?),
            aes: None,
        })
    }

    /// AES-256-GCM envelope encryption with a 32-byte data key; `key_ref`
    /// is what readers need to recover the key (e.g. the KMS `CiphertextBlob`).
    #[staticmethod]
    #[pyo3(signature = (data_key, *, key_ref))]
    fn aes_256_gcm(data_key: Vec<u8>, key_ref: String) -> PyResult<Self> {
        let aes = Arc::new(Aes256GcmEncryptor::new(&data_key, key_ref)?);
        Ok(Self {
            inner: Arc::clone(&aes) as Arc<dyn FieldProtector>,
            aes: Some(aes),
        })
    }

    /// Any object with `scheme`, optional `key_ref` and
    /// `protect_batch(tenant_id, field, values) -> list[str]`.
    #[staticmethod]
    fn custom(protector: &Bound<'_, PyAny>) -> PyResult<Self> {
        let scheme: String = protector.getattr("scheme")?.extract()?;
        validate_scheme(&scheme)?;
        let key_ref: Option<String> = match protector.getattr("key_ref") {
            Ok(v) => v.extract()?,
            Err(_) => None,
        };
        if !protector.getattr("protect_batch")?.is_callable() {
            return Err(PyTypeError::new_err(
                "protector.protect_batch must be callable",
            ));
        }
        Ok(Self {
            inner: Arc::new(PyFieldProtector {
                object: protector.clone().unbind(),
                scheme,
                key_ref,
            }),
            aes: None,
        })
    }

    /// Scheme name recorded in `_dl_pii_protection`.
    #[getter]
    fn scheme(&self) -> String {
        self.inner.scheme().to_owned()
    }

    /// Key reference recorded in `_dl_pii_key_ref`, if the scheme has one.
    #[getter]
    fn key_ref(&self) -> Option<String> {
        self.inner.key_ref().map(str::to_owned)
    }

    /// Decrypts an `aes-256-gcm` value written for `tenant_id` and `field`.
    #[pyo3(signature = (token, *, tenant_id, field))]
    fn decrypt(&self, token: &str, tenant_id: &str, field: &str) -> PyResult<String> {
        let aes = self.aes.as_ref().ok_or_else(|| {
            DatalakeError::config(format!(
                "scheme '{}' is not reversible by the SDK",
                self.inner.scheme()
            ))
        })?;
        Ok(aes.decrypt(&ProtectionContext { tenant_id, field }, token)?)
    }

    fn __repr__(&self) -> String {
        format!("PiiProtection(scheme='{}')", self.inner.scheme())
    }
}

/// Where a tenant's data lives per medallion layer, and how its PII is protected.
#[pyclass(
    name = "TenantProfile",
    module = "datalake_medallion_sdk._core",
    frozen
)]
pub(crate) struct PyTenantProfile {
    pub(crate) inner: Arc<TenantProfile>,
}

fn order(value: &str) -> Result<PartitionOrder, DatalakeError> {
    PartitionOrder::parse(value)
}

#[pymethods]
impl PyTenantProfile {
    #[new]
    #[pyo3(signature = (
        tenant_id,
        *,
        bronze,
        quarantine,
        silver = None,
        gold = None,
        partition_order = "tenant_first",
        pii_protection = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        tenant_id: &str,
        bronze: &str,
        quarantine: &str,
        silver: Option<&str>,
        gold: Option<&str>,
        partition_order: &str,
        pii_protection: Option<&PyPiiProtection>,
    ) -> PyResult<Self> {
        let mut builder = TenantProfile::builder(TenantId::new(tenant_id)?)
            .location(Layer::Bronze, StorageLocation::parse(bronze)?)
            .location(Layer::Quarantine, StorageLocation::parse(quarantine)?)
            .partition_order(order(partition_order)?)
            .pii_protection(pii_protection.map(|p| Arc::clone(&p.inner)));
        for (layer, uri) in [(Layer::Silver, silver), (Layer::Gold, gold)] {
            if let Some(uri) = uri {
                builder = builder.location(layer, StorageLocation::parse(uri)?);
            }
        }
        Ok(Self {
            inner: Arc::new(builder.build()?),
        })
    }

    /// Every layer in one bucket under `bronze/`, `quarantine/`, `silver/` and `gold/`.
    #[staticmethod]
    #[pyo3(signature = (tenant_id, bucket, *, partition_order = "tenant_first", pii_protection = None))]
    fn for_bucket(
        tenant_id: &str,
        bucket: &str,
        partition_order: &str,
        pii_protection: Option<&PyPiiProtection>,
    ) -> PyResult<Self> {
        let profile = TenantProfile::for_bucket(TenantId::new(tenant_id)?, bucket)?
            .partition_order(order(partition_order)?)
            .pii_protection(pii_protection.map(|p| Arc::clone(&p.inner)))
            .build()?;
        Ok(Self {
            inner: Arc::new(profile),
        })
    }

    /// The tenant.
    #[getter]
    fn tenant_id(&self) -> String {
        self.inner.tenant_id().to_string()
    }

    /// `tenant_first` or `data_type_first`.
    #[getter]
    fn partition_order(&self) -> &'static str {
        self.inner.partition_order().as_str()
    }

    /// PII scheme name, or `none`.
    #[getter]
    fn pii_protection(&self) -> String {
        self.inner.pii_scheme().to_owned()
    }

    /// `s3://bucket/prefix` of `layer`, or `None` when the profile has none.
    fn location(&self, layer: &str) -> PyResult<Option<String>> {
        Ok(self
            .inner
            .location(Layer::parse(layer)?)
            .map(StorageLocation::uri))
    }

    /// `s3://` prefix of this tenant's `data_type` table in `layer`.
    fn table_uri(&self, layer: &str, data_type: &str) -> PyResult<Option<String>> {
        Ok(self.inner.table_uri(Layer::parse(layer)?, data_type)?)
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self.inner)
    }
}

/// Immutable directory of tenant profiles, resolved by tenant id on each call.
#[pyclass(
    name = "TenantRegistry",
    module = "datalake_medallion_sdk._core",
    frozen
)]
pub(crate) struct PyTenantRegistry {
    pub(crate) inner: Arc<TenantRegistry>,
}

#[pymethods]
impl PyTenantRegistry {
    #[new]
    fn new(profiles: Vec<PyRef<'_, PyTenantProfile>>) -> PyResult<Self> {
        let registry = TenantRegistry::new(profiles.iter().map(|p| Arc::clone(&p.inner)))?;
        Ok(Self {
            inner: Arc::new(registry),
        })
    }

    /// Profile of `tenant_id`; raises `TenantViolationError` when unknown.
    fn get(&self, tenant_id: &str) -> PyResult<PyTenantProfile> {
        Ok(PyTenantProfile {
            inner: self.inner.resolve(tenant_id)?,
        })
    }

    /// Registered tenant ids, sorted.
    fn tenant_ids(&self) -> Vec<String> {
        self.inner.tenant_ids().map(str::to_owned).collect()
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn __contains__(&self, tenant_id: &str) -> bool {
        self.inner.resolve(tenant_id).is_ok()
    }

    fn __repr__(&self) -> String {
        format!("TenantRegistry(tenants={})", self.inner.len())
    }
}
