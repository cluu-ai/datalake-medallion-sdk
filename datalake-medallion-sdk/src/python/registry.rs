use std::path::PathBuf;
use std::sync::Arc;

use pyo3::prelude::*;
use serde_json::Value;

use crate::contracts::{
    ContractRegistry, EmbeddedSchemaProvider, FileSystemSchemaProvider, DEFAULT_EXTENSION_PREFIX,
};
use crate::error::DatalakeError;

/// Immutable set of compiled data contracts.
#[pyclass(
    name = "ContractRegistry",
    module = "datalake_medallion_sdk._core",
    frozen
)]
pub(crate) struct PyContractRegistry {
    pub(crate) inner: Arc<ContractRegistry>,
}

#[pymethods]
impl PyContractRegistry {
    /// Loads every `*.schema.json` under `path` (recursively, skipping `_`-prefixed entries).
    #[staticmethod]
    #[pyo3(signature = (path, *, extension_prefix = DEFAULT_EXTENSION_PREFIX))]
    fn from_directory(py: Python<'_>, path: PathBuf, extension_prefix: &str) -> PyResult<Self> {
        let provider = FileSystemSchemaProvider::new(path);
        let prefix = extension_prefix.to_owned();
        let registry = py.detach(move || ContractRegistry::from_provider(&provider, &prefix))?;
        Ok(Self {
            inner: Arc::new(registry),
        })
    }

    /// Compiles schemas given as dicts or JSON strings.
    #[staticmethod]
    #[pyo3(signature = (schemas, *, extension_prefix = DEFAULT_EXTENSION_PREFIX))]
    fn from_schemas(
        py: Python<'_>,
        schemas: &Bound<'_, PyAny>,
        extension_prefix: &str,
    ) -> PyResult<Self> {
        let json = py.import("json")?;
        let mut provider = EmbeddedSchemaProvider::new();
        for (i, item) in schemas.try_iter()?.enumerate() {
            let item = item?;
            let text: String = match item.extract::<String>() {
                Ok(s) => s,
                Err(_) => json.call_method1("dumps", (&item,))?.extract()?,
            };
            let value: Value = serde_json::from_str(&text)
                .map_err(|e| DatalakeError::boot(format!("schema #{i} is not valid JSON: {e}")))?;
            provider = provider.with_schema(format!("schema #{i}"), value);
        }
        Ok(Self {
            inner: Arc::new(ContractRegistry::from_provider(
                &provider,
                extension_prefix,
            )?),
        })
    }

    /// Registered contract keys (`domain/datatype/version`), sorted.
    fn keys(&self) -> Vec<String> {
        self.inner.keys().map(str::to_owned).collect()
    }

    /// PII fields of a contract.
    fn pii_fields(&self, key: &str) -> PyResult<Vec<String>> {
        Ok(self.inner.get(key)?.pii_fields().to_vec())
    }

    /// SHA-256 of a contract document, as pinned in the envelope.
    fn schema_hash(&self, key: &str) -> PyResult<String> {
        Ok(self.inner.get(key)?.schema_hash().to_owned())
    }

    /// Flexibility mode of a contract.
    fn flexibility_mode(&self, key: &str) -> PyResult<&'static str> {
        Ok(self.inner.get(key)?.mode().as_str())
    }

    /// Keyword namespace used by the contracts.
    #[getter]
    fn extension_prefix(&self) -> String {
        self.inner.extension_prefix().to_owned()
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn __contains__(&self, key: &str) -> bool {
        self.inner.contains(key)
    }

    fn __repr__(&self) -> String {
        format!(
            "ContractRegistry(contracts={}, extension_prefix={:?})",
            self.inner.len(),
            self.inner.extension_prefix()
        )
    }
}
