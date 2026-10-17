use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::pyarrow::FromPyArrow;
use arrow::record_batch::RecordBatch;
use chrono::Duration;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::sync::OnceCell;

use super::hooks::PyRecordHook;
use super::registry::PyContractRegistry;
use crate::contracts::ContractRegistry;
use crate::envelope::{EnvelopePrefix, DEFAULT_ENVELOPE_PREFIX};
use crate::error::DatalakeError;
use crate::pipeline::{
    BronzePipeline, IngestReport, IngestRequest, InvalidRecordPolicy, PipelineConfig, RecordSource,
    DEFAULT_MAX_RECORD_BYTES,
};
use crate::storage::{InMemoryStorage, S3Options, S3Storage, StaticCredentials};
use crate::tenant::path::{DEFAULT_DATA_PREFIX, DEFAULT_DLQ_PREFIX};
use crate::tenant::{BronzePathResolver, PartitionOrder, TenantId};
use crate::validation::hooks::RecordHook;
use crate::validation::pii::PiiHasher;

/// Outcome of `BronzeIngestionClient.ingest`.
#[pyclass(
    name = "IngestReport",
    module = "datalake_medallion_sdk._core",
    frozen,
    get_all
)]
pub(crate) struct PyIngestReport {
    batch_id: String,
    records_received: usize,
    records_ok: usize,
    records_quarantined: usize,
    rejections: BTreeMap<String, usize>,
    data_uri: Option<String>,
    dlq_uris: Vec<String>,
    pii_masking: String,
}

impl From<IngestReport> for PyIngestReport {
    fn from(r: IngestReport) -> Self {
        Self {
            batch_id: r.batch_id,
            records_received: r.records_received,
            records_ok: r.records_ok,
            records_quarantined: r.records_quarantined,
            rejections: r.rejections,
            data_uri: r.data_uri,
            dlq_uris: r.dlq_uris,
            pii_masking: r.pii_masking,
        }
    }
}

#[pymethods]
impl PyIngestReport {
    fn __repr__(&self) -> String {
        format!(
            "IngestReport(batch_id={:?}, records_received={}, records_ok={}, records_quarantined={})",
            self.batch_id, self.records_received, self.records_ok, self.records_quarantined
        )
    }
}

struct Parts {
    registry: Arc<ContractRegistry>,
    paths: BronzePathResolver,
    config: PipelineConfig,
    s3: S3Options,
}

/// Validating Bronze writer for S3 and S3-compatible object stores.
#[pyclass(
    name = "BronzeIngestionClient",
    module = "datalake_medallion_sdk._core",
    frozen
)]
pub(crate) struct PyBronzeIngestionClient {
    parts: Arc<Parts>,
    hooks: HashMap<String, Arc<dyn RecordHook>>,
    pipeline: Arc<OnceCell<Arc<BronzePipeline>>>,
}

#[pymethods]
impl PyBronzeIngestionClient {
    #[new]
    #[pyo3(signature = (
        bucket,
        registry,
        *,
        data_prefix = DEFAULT_DATA_PREFIX,
        dlq_prefix = DEFAULT_DLQ_PREFIX,
        partition_order = "tenant_first",
        envelope_prefix = DEFAULT_ENVELOPE_PREFIX,
        max_record_bytes = DEFAULT_MAX_RECORD_BYTES,
        max_future_skew_seconds = Some(3600),
        pii_masking = true,
        pii_hmac_key = None,
        on_invalid = "quarantine",
        hooks = None,
        aws_region = None,
        aws_endpoint_url = None,
        aws_access_key_id = None,
        aws_secret_access_key = None,
        aws_session_token = None,
        force_path_style = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        bucket: String,
        registry: &PyContractRegistry,
        data_prefix: &str,
        dlq_prefix: &str,
        partition_order: &str,
        envelope_prefix: &str,
        max_record_bytes: usize,
        max_future_skew_seconds: Option<i64>,
        pii_masking: bool,
        pii_hmac_key: Option<Vec<u8>>,
        on_invalid: &str,
        hooks: Option<&Bound<'_, PyDict>>,
        aws_region: Option<String>,
        aws_endpoint_url: Option<String>,
        aws_access_key_id: Option<String>,
        aws_secret_access_key: Option<String>,
        aws_session_token: Option<String>,
        force_path_style: Option<bool>,
    ) -> PyResult<Self> {
        let paths = BronzePathResolver::new(bucket.clone())?
            .with_data_prefix(data_prefix)?
            .with_dlq_prefix(dlq_prefix)?
            .with_partition_order(PartitionOrder::parse(partition_order)?);

        let invalid_records = match on_invalid {
            "quarantine" => InvalidRecordPolicy::Quarantine,
            "raise" => InvalidRecordPolicy::Reject,
            other => {
                return Err(DatalakeError::config(format!(
                    "on_invalid must be \"quarantine\" or \"raise\", got {other:?}"
                ))
                .into())
            }
        };
        let config = PipelineConfig {
            envelope_prefix: EnvelopePrefix::new(envelope_prefix)?,
            max_record_bytes,
            max_future_skew: max_future_skew_seconds.map(Duration::seconds),
            pii_hasher: match pii_hmac_key {
                Some(key) => PiiHasher::hmac_sha256(&key)?,
                None => PiiHasher::Sha256,
            },
            mask_pii: pii_masking,
            invalid_records,
            ..PipelineConfig::default()
        };

        let credentials = match (aws_access_key_id, aws_secret_access_key) {
            (Some(access_key_id), Some(secret_access_key)) => Some(StaticCredentials {
                access_key_id,
                secret_access_key,
                session_token: aws_session_token,
            }),
            (None, None) if aws_session_token.is_none() => None,
            _ => {
                return Err(DatalakeError::config(
                    "aws_access_key_id and aws_secret_access_key must be given together \
                     (aws_session_token requires both)",
                )
                .into())
            }
        };

        let registry = Arc::clone(&registry.inner);
        let mut bound = HashMap::new();
        if let Some(hooks) = hooks {
            for (key, callable) in hooks.iter() {
                let key: String = key.extract()?;
                if !registry.contains(&key) {
                    return Err(DatalakeError::boot(format!(
                        "hook bound to unknown contract '{key}'"
                    ))
                    .into());
                }
                if !callable.is_callable() {
                    return Err(PyTypeError::new_err(format!(
                        "hook for contract '{key}' is not callable"
                    )));
                }
                let hook: Arc<dyn RecordHook> = Arc::new(PyRecordHook::new(callable.unbind()));
                bound.insert(key, hook);
            }
        }

        // Runs the boot checks (reserved fields, limits) before any network setup.
        BronzePipeline::new(
            Arc::clone(&registry),
            Arc::new(InMemoryStorage::new()),
            paths.clone(),
            config.clone(),
        )?;

        Ok(Self {
            parts: Arc::new(Parts {
                registry,
                paths,
                config,
                s3: S3Options {
                    bucket,
                    region: aws_region,
                    endpoint_url: aws_endpoint_url,
                    force_path_style,
                    credentials,
                },
            }),
            hooks: bound,
            pipeline: Arc::new(OnceCell::new()),
        })
    }

    /// Validates `batch` against `contract` and writes it for `tenant_id`.
    ///
    /// `batch` may be a dict, a list of dicts, a JSON string or bytes, or any
    /// object implementing the Arrow `PyCapsule` interface (`__arrow_c_stream__`
    /// or `__arrow_c_array__`), such as a `pyarrow` table or record batch or a
    /// Polars data frame.
    #[pyo3(signature = (batch, *, tenant_id, contract, pii_masking = None))]
    fn ingest<'py>(
        &self,
        py: Python<'py>,
        batch: &Bound<'py, PyAny>,
        tenant_id: &str,
        contract: &str,
        pii_masking: Option<bool>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let tenant = TenantId::new(tenant_id)?;
        self.parts.registry.get(contract)?;
        let records = to_source(py, batch)?;
        let mut request = IngestRequest::new(tenant, contract, records);
        if let Some(enabled) = pii_masking {
            request = request.with_pii_masking(enabled);
        }
        if let Some(hook) = self.hooks.get(contract) {
            request = request.with_hook(Arc::clone(hook));
        }

        let parts = Arc::clone(&self.parts);
        let cell = Arc::clone(&self.pipeline);
        future_into_py(py, async move {
            let pipeline = cell
                .get_or_try_init(|| async {
                    let storage = S3Storage::connect(parts.s3.clone()).await;
                    BronzePipeline::new(
                        Arc::clone(&parts.registry),
                        Arc::new(storage),
                        parts.paths.clone(),
                        parts.config.clone(),
                    )
                    .map(Arc::new)
                })
                .await?;
            let worker = Arc::clone(pipeline);
            let prepared = tokio::task::spawn_blocking(move || worker.prepare(request))
                .await
                .map_err(|e| {
                    DatalakeError::storage("runtime", format!("validation task failed: {e}"))
                })??;
            let report = pipeline.commit(prepared).await?;
            Ok(PyIngestReport::from(report))
        })
    }

    /// Target bucket.
    #[getter]
    fn bucket(&self) -> String {
        self.parts.paths.bucket().to_owned()
    }

    /// Contracts that have a hook bound.
    #[getter]
    fn hooked_contracts(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.hooks.keys().cloned().collect();
        keys.sort();
        keys
    }

    fn __repr__(&self) -> String {
        let p = &self.parts;
        format!(
            "BronzeIngestionClient(bucket={:?}, data_prefix={:?}, dlq_prefix={:?}, contracts={}, pii={})",
            p.paths.bucket(),
            p.paths.data_prefix(),
            p.paths.dlq_prefix(),
            p.registry.len(),
            if p.config.mask_pii { p.config.pii_hasher.algorithm() } else { "none" }
        )
    }
}

fn to_source(py: Python<'_>, batch: &Bound<'_, PyAny>) -> PyResult<RecordSource> {
    let arrow_err = |e: arrow::error::ArrowError| DatalakeError::MalformedPayload {
        reason: format!("cannot read Arrow input: {e}"),
    };
    if batch.hasattr("__arrow_c_stream__")? {
        let reader = ArrowArrayStreamReader::from_pyarrow_bound(batch)?;
        let batches = reader
            .collect::<Result<Vec<RecordBatch>, _>>()
            .map_err(arrow_err)?;
        return Ok(RecordSource::Arrow(batches));
    }
    if batch.hasattr("__arrow_c_array__")? {
        return Ok(RecordSource::Arrow(vec![RecordBatch::from_pyarrow_bound(
            batch,
        )?]));
    }
    if batch.is_instance_of::<PyString>() {
        return Ok(RecordSource::Json(batch.extract()?));
    }
    if batch.is_instance_of::<PyBytes>() {
        let raw: Vec<u8> = batch.extract()?;
        let text = String::from_utf8(raw).map_err(|e| DatalakeError::MalformedPayload {
            reason: format!("bytes input is not UTF-8: {e}"),
        })?;
        return Ok(RecordSource::Json(text));
    }
    if batch.is_instance_of::<PyDict>()
        || batch.is_instance_of::<PyList>()
        || batch.is_instance_of::<PyTuple>()
    {
        let kwargs = PyDict::new(py);
        kwargs.set_item("allow_nan", false)?;
        kwargs.set_item("ensure_ascii", false)?;
        // Compact text: size limits and `payload_bytes` count data, not
        // formatting, the same as for JSON and Arrow input.
        kwargs.set_item("separators", (",", ":"))?;
        let text = py
            .import("json")?
            .call_method("dumps", (batch,), Some(&kwargs))
            .map_err(|e| DatalakeError::MalformedPayload {
                reason: format!("batch is not JSON-serializable: {e}"),
            })?;
        return Ok(RecordSource::Json(text.extract()?));
    }
    Err(PyTypeError::new_err(
        "batch must be a dict, a list of dicts, a JSON str/bytes, or an Arrow PyCapsule object",
    ))
}
