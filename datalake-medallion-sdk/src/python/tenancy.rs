//! Python classes for tenant profiles and PII protection schemes.

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyList;
use zeroize::Zeroizing;

use crate::error::DatalakeError;
use crate::tenant::{
    Layer, PartitionOrder, StorageLocation, TenantDirectory, TenantId, TenantProfile,
    TenantRegistry,
};
use crate::validation::keys::{
    DataKey, DataKeyProvider, EnvelopeProtector, KeyCachePolicy, PiiDecryptor,
};
use crate::validation::pii::{
    validate_scheme, Aes256GcmEncryptor, FieldProtector, HmacSha256Hasher, ProtectionContext,
    Sha256Hasher,
};

/// Adapts a Python object with `generate(tenant_id) -> (bytes, str)` and
/// `decrypt(tenant_id, key_ref) -> bytes` to [`DataKeyProvider`]. Called about
/// once per tenant and cache period; exceptions are reported by type only.
struct PyDataKeyProvider {
    object: Py<PyAny>,
    name: String,
}

impl fmt::Debug for PyDataKeyProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PyDataKeyProvider({})", self.name)
    }
}

fn py_failure(py: Python<'_>, err: &PyErr, what: &str) -> DatalakeError {
    let name = err
        .get_type(py)
        .name()
        .map_or_else(|_| "exception".to_owned(), |n| n.to_string());
    DatalakeError::protection(format!("data key provider {what} raised {name}"))
}

impl DataKeyProvider for PyDataKeyProvider {
    fn name(&self) -> &str {
        &self.name
    }

    fn generate(&self, tenant_id: &str) -> Result<DataKey, DatalakeError> {
        Python::try_attach(|py| {
            let out = self
                .object
                .bind(py)
                .call_method1("generate", (tenant_id,))
                .and_then(|r| r.extract::<(Vec<u8>, String)>())
                .map_err(|e| py_failure(py, &e, "generate"))?;
            Ok(DataKey {
                plaintext: Zeroizing::new(out.0),
                key_ref: out.1,
            })
        })
        .unwrap_or_else(|| Err(DatalakeError::protection("Python interpreter unavailable")))
    }

    fn decrypt(&self, tenant_id: &str, key_ref: &str) -> Result<Zeroizing<Vec<u8>>, DatalakeError> {
        Python::try_attach(|py| {
            self.object
                .bind(py)
                .call_method1("decrypt", (tenant_id, key_ref))
                .and_then(|r| r.extract::<Vec<u8>>())
                .map(Zeroizing::new)
                .map_err(|e| py_failure(py, &e, "decrypt"))
        })
        .unwrap_or_else(|| Err(DatalakeError::protection("Python interpreter unavailable")))
    }
}

fn py_provider(provider: &Bound<'_, PyAny>) -> PyResult<Arc<dyn DataKeyProvider>> {
    for method in ["generate", "decrypt"] {
        if !provider.getattr(method)?.is_callable() {
            return Err(PyTypeError::new_err(format!(
                "provider.{method} must be callable"
            )));
        }
    }
    let name = match provider.getattr("name") {
        Ok(n) => n.extract::<String>()?,
        Err(_) => provider.get_type().name()?.to_string(),
    };
    Ok(Arc::new(PyDataKeyProvider {
        object: provider.clone().unbind(),
        name,
    }))
}

#[cfg(feature = "aws-kms")]
fn kms_provider(
    key_id: &str,
    region: Option<String>,
    endpoint_url: Option<String>,
) -> PyResult<Arc<dyn DataKeyProvider>> {
    use crate::validation::aws_kms::{AwsKmsDataKeyProvider, KmsOptions};
    Ok(Arc::new(AwsKmsDataKeyProvider::new(
        key_id,
        KmsOptions {
            region,
            endpoint_url,
        },
    )?))
}

fn cache_policy(
    data_key_max_age: f64,
    max_nonce_prefixes_per_key: u64,
) -> PyResult<KeyCachePolicy> {
    if !data_key_max_age.is_finite() || data_key_max_age <= 0.0 {
        return Err(
            DatalakeError::config("data_key_max_age must be a positive number of seconds").into(),
        );
    }
    let policy = KeyCachePolicy {
        max_age: Duration::from_secs_f64(data_key_max_age),
        max_nonces: max_nonce_prefixes_per_key,
        ..KeyCachePolicy::default()
    };
    policy.validate()?;
    Ok(policy)
}

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

    /// AES-256-GCM with a data key per tenant from an AWS KMS key, reused until
    /// it is `data_key_max_age` seconds old (default 1 hour) or has drawn
    /// `max_nonce_prefixes_per_key` nonces (default 2^15).
    #[cfg(feature = "aws-kms")]
    #[staticmethod]
    #[pyo3(signature = (kms_key_id, *, data_key_max_age = 3600.0, max_nonce_prefixes_per_key = 32768, region = None, endpoint_url = None))]
    fn kms_envelope(
        kms_key_id: &str,
        data_key_max_age: f64,
        max_nonce_prefixes_per_key: u64,
        region: Option<String>,
        endpoint_url: Option<String>,
    ) -> PyResult<Self> {
        let provider = kms_provider(kms_key_id, region, endpoint_url)?;
        let policy = cache_policy(data_key_max_age, max_nonce_prefixes_per_key)?;
        Ok(Self {
            inner: Arc::new(EnvelopeProtector::new(provider, policy)?),
            aes: None,
        })
    }

    /// AES-256-GCM with data keys from any provider object implementing
    /// `generate(tenant_id)` and `decrypt(tenant_id, key_ref)`.
    #[staticmethod]
    #[pyo3(signature = (provider, *, data_key_max_age = 3600.0, max_nonce_prefixes_per_key = 32768))]
    fn envelope(
        provider: &Bound<'_, PyAny>,
        data_key_max_age: f64,
        max_nonce_prefixes_per_key: u64,
    ) -> PyResult<Self> {
        let policy = cache_policy(data_key_max_age, max_nonce_prefixes_per_key)?;
        Ok(Self {
            inner: Arc::new(EnvelopeProtector::new(py_provider(provider)?, policy)?),
            aes: None,
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

    /// Registry from a configuration document: a dict, or the path of a JSON file.
    #[staticmethod]
    fn from_config(py: Python<'_>, config: &Bound<'_, PyAny>) -> PyResult<Self> {
        let value: serde_json::Value = if let Ok(path) = config.extract::<PathBuf>() {
            let text = std::fs::read_to_string(&path).map_err(|e| {
                DatalakeError::config(format!(
                    "cannot read tenant configuration {}: {e}",
                    path.display()
                ))
            })?;
            serde_json::from_str(&text)
        } else {
            let text: String = py
                .import("json")?
                .call_method1("dumps", (config,))?
                .extract()?;
            serde_json::from_str(&text)
        }
        .map_err(|e| {
            DatalakeError::config(format!("tenant configuration is not valid JSON: {e}"))
        })?;
        let registry = py.detach(|| TenantRegistry::from_config(&value))?;
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

/// Reads AES-256-GCM PII values back: recovers each data key once through its
/// provider and caches it per tenant and key reference.
#[pyclass(name = "PiiDecryptor", module = "datalake_medallion_sdk._core", frozen)]
pub(crate) struct PyPiiDecryptor {
    inner: Arc<PiiDecryptor>,
}

fn decryptor(
    provider: Arc<dyn DataKeyProvider>,
    cache_size: usize,
    max_age: f64,
) -> PyResult<PyPiiDecryptor> {
    if !max_age.is_finite() || max_age <= 0.0 {
        return Err(DatalakeError::config("max_age must be a positive number of seconds").into());
    }
    Ok(PyPiiDecryptor {
        inner: Arc::new(PiiDecryptor::new(
            provider,
            cache_size,
            Duration::from_secs_f64(max_age),
        )?),
    })
}

#[pymethods]
impl PyPiiDecryptor {
    /// Decryptor for keys issued by AWS KMS (`kms1:` key references).
    #[cfg(feature = "aws-kms")]
    #[staticmethod]
    #[pyo3(signature = (*, region = None, endpoint_url = None, cache_size = 1024, max_age = 3600.0))]
    fn kms(
        region: Option<String>,
        endpoint_url: Option<String>,
        cache_size: usize,
        max_age: f64,
    ) -> PyResult<Self> {
        // The key id comes from each key reference; this one is a placeholder.
        decryptor(
            kms_provider("from-key-ref", region, endpoint_url)?,
            cache_size,
            max_age,
        )
    }

    /// Decryptor for keys from a provider object (`generate`/`decrypt`).
    #[staticmethod]
    #[pyo3(signature = (provider, *, cache_size = 1024, max_age = 3600.0))]
    fn from_provider(
        provider: &Bound<'_, PyAny>,
        cache_size: usize,
        max_age: f64,
    ) -> PyResult<Self> {
        decryptor(py_provider(provider)?, cache_size, max_age)
    }

    /// Decrypts one token written for `tenant_id` and `field` with `key_ref`.
    #[pyo3(signature = (token, *, tenant_id, field, key_ref))]
    fn decrypt(
        &self,
        py: Python<'_>,
        token: &str,
        tenant_id: &str,
        field: &str,
        key_ref: &str,
    ) -> PyResult<String> {
        let inner = Arc::clone(&self.inner);
        Ok(py.detach(|| inner.decrypt(tenant_id, field, key_ref, token))?)
    }

    /// Decrypts a column (``None`` stays ``None``) without holding the GIL.
    #[pyo3(signature = (tokens, *, tenant_id, field, key_ref))]
    fn decrypt_many(
        &self,
        py: Python<'_>,
        tokens: Vec<Option<String>>,
        tenant_id: &str,
        field: &str,
        key_ref: &str,
    ) -> PyResult<Vec<Option<String>>> {
        let inner = Arc::clone(&self.inner);
        Ok(py.detach(|| {
            let refs: Vec<Option<&str>> = tokens.iter().map(Option::as_deref).collect();
            inner.decrypt_many(tenant_id, field, key_ref, &refs)
        })?)
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self.inner)
    }
}
