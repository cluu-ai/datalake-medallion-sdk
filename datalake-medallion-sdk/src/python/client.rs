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
use super::tenancy::{PyTenantProfile, PyTenantRegistry};
use crate::contracts::ContractRegistry;
use crate::envelope::{EnvelopePrefix, DEFAULT_ENVELOPE_PREFIX};
use crate::error::DatalakeError;
use crate::pipeline::{
    BronzePipeline, IngestReport, IngestRequest, InvalidRecordPolicy, PipelineConfig, RecordSource,
    DEFAULT_MAX_RECORD_BYTES,
};
use crate::storage::{InMemoryStorage, S3Options, S3Storage, StaticCredentials};
use crate::stream::{BronzeStream, StreamOptions, StreamReport};
use crate::tenant::{TenantDirectory, TenantProfile, TenantRegistry};
use crate::validation::hooks::RecordHook;

/// Outcome of `BronzeIngestionClient.ingest`.
#[pyclass(
    name = "IngestReport",
    module = "datalake_medallion_sdk._core",
    frozen,
    get_all
)]
pub(crate) struct PyIngestReport {
    batch_id: String,
    tenant_id: String,
    records_received: usize,
    records_ok: usize,
    records_quarantined: usize,
    rejections: BTreeMap<String, usize>,
    data_uri: Option<String>,
    data_uris: Vec<String>,
    dlq_uris: Vec<String>,
    pii_protection: String,
}

/// Per-tenant result of `ingest_otel_logs`, converted to a Python tuple
/// `(tenant, sources, report | None, exception | None)`.
struct OtelOutcome {
    tenant: Option<String>,
    sources: Vec<usize>,
    report: Result<IngestReport, OtelError>,
}

/// Why a tenant of `ingest_otel_logs` was not written.
enum OtelError {
    Datalake(DatalakeError),
    /// The tenant could not be resolved (an exception of `resolve`).
    Python(PyErr),
}

impl<'py> IntoPyObject<'py> for OtelOutcome {
    type Target = PyTuple;
    type Output = Bound<'py, PyTuple>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let (report, error): (Option<PyIngestReport>, Option<Py<PyAny>>) = match self.report {
            Ok(r) => (Some(PyIngestReport::from(r)), None),
            Err(OtelError::Datalake(e)) => (None, Some(PyErr::from(e).into_value(py).into_any())),
            Err(OtelError::Python(e)) => (None, Some(e.into_value(py).into_any())),
        };
        (self.tenant, self.sources, report, error).into_pyobject(py)
    }
}

impl From<IngestReport> for PyIngestReport {
    fn from(r: IngestReport) -> Self {
        Self {
            batch_id: r.batch_id,
            tenant_id: r.tenant_id,
            records_received: r.records_received,
            records_ok: r.records_ok,
            records_quarantined: r.records_quarantined,
            rejections: r.rejections,
            data_uri: r.data_uri,
            data_uris: r.data_uris,
            dlq_uris: r.dlq_uris,
            pii_protection: r.pii_protection,
        }
    }
}

/// Outcome of `BronzeIngestionClient.ingest_stream`.
#[pyclass(
    name = "StreamReport",
    module = "datalake_medallion_sdk._core",
    frozen,
    get_all
)]
pub(crate) struct PyStreamReport {
    stream_id: String,
    tenant_id: String,
    records_received: usize,
    records_ok: usize,
    records_quarantined: usize,
    rejections: BTreeMap<String, usize>,
    data_uris: Vec<String>,
    dlq_uris: Vec<String>,
    bytes_written: u64,
    pii_protection: String,
}

impl From<StreamReport> for PyStreamReport {
    fn from(r: StreamReport) -> Self {
        Self {
            stream_id: r.stream_id,
            tenant_id: r.tenant_id,
            records_received: r.records_received,
            records_ok: r.records_ok,
            records_quarantined: r.records_quarantined,
            rejections: r.rejections,
            data_uris: r.data_uris,
            dlq_uris: r.dlq_uris,
            bytes_written: r.bytes_written,
            pii_protection: r.pii_protection,
        }
    }
}

#[pymethods]
impl PyStreamReport {
    fn __repr__(&self) -> String {
        format!(
            "StreamReport(tenant_id={:?}, stream_id={:?}, records_received={}, records_ok={}, records_quarantined={}, objects={})",
            self.tenant_id,
            self.stream_id,
            self.records_received,
            self.records_ok,
            self.records_quarantined,
            self.data_uris.len()
        )
    }
}

/// Where a stream's chunks come from.
enum StreamInput {
    /// An Arrow C stream, read batch by batch without the GIL.
    Arrow(ArrowArrayStreamReader),
    /// A Python iterator of chunks, each converted like an `ingest` batch.
    Iter(Py<PyAny>),
}

#[pymethods]
impl PyIngestReport {
    fn __repr__(&self) -> String {
        format!(
            "IngestReport(tenant_id={:?}, batch_id={:?}, records_received={}, records_ok={}, records_quarantined={})",
            self.tenant_id, self.batch_id, self.records_received, self.records_ok, self.records_quarantined
        )
    }
}

struct Parts {
    registry: Arc<ContractRegistry>,
    tenants: Option<Arc<TenantRegistry>>,
    config: PipelineConfig,
    s3: S3Options,
}

/// Validating Bronze writer for S3 and S3-compatible object stores. Holds no
/// destination: each call names its tenant, whose profile decides where the
/// records go and how their PII is protected.
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
        registry,
        *,
        tenants = None,
        envelope_prefix = DEFAULT_ENVELOPE_PREFIX,
        max_record_bytes = DEFAULT_MAX_RECORD_BYTES,
        max_future_skew_seconds = Some(3600),
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
        registry: &PyContractRegistry,
        tenants: Option<&PyTenantRegistry>,
        envelope_prefix: &str,
        max_record_bytes: usize,
        max_future_skew_seconds: Option<i64>,
        on_invalid: &str,
        hooks: Option<&Bound<'_, PyDict>>,
        aws_region: Option<String>,
        aws_endpoint_url: Option<String>,
        aws_access_key_id: Option<String>,
        aws_secret_access_key: Option<String>,
        aws_session_token: Option<String>,
        force_path_style: Option<bool>,
    ) -> PyResult<Self> {
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
            config.clone(),
        )?;

        Ok(Self {
            parts: Arc::new(Parts {
                registry,
                tenants: tenants.map(|t| Arc::clone(&t.inner)),
                config,
                s3: S3Options {
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

    /// Validates `batch` against `contract` and writes it for `tenant`: a
    /// `TenantProfile`, or a tenant id resolved in the client's `tenants`.
    ///
    /// `batch` may be a dict, a list of dicts, a JSON string or bytes, or any
    /// object implementing the Arrow `PyCapsule` interface (`__arrow_c_stream__`
    /// or `__arrow_c_array__`), such as a `pyarrow` table or record batch or a
    /// Polars data frame.
    #[pyo3(signature = (batch, *, tenant, contract, batch_id = None))]
    fn ingest<'py>(
        &self,
        py: Python<'py>,
        batch: &Bound<'py, PyAny>,
        tenant: &Bound<'py, PyAny>,
        contract: &str,
        batch_id: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let tenant = self.resolve(tenant)?;
        self.parts.registry.get(contract)?;
        let records = to_source(py, batch)?;
        let mut request = IngestRequest::new(tenant, contract, records);
        if let Some(id) = batch_id {
            request = request.with_batch_id(id);
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
            // Uploads rewrite the same keys, so transient failures are
            // retried here rather than by re-validating with a new batch id.
            let report = pipeline.commit_with_retries(&prepared, 4).await?;
            Ok(PyIngestReport::from(report))
        })
    }

    /// Flattens an OTLP/JSON logs request and ingests each tenant's records
    /// in one call, without pyarrow and without holding the GIL while parsing.
    ///
    /// `resolve(tenant_or_none)` maps each tenant found in `tenant_attribute`
    /// (or `None` for records without it) to a `TenantProfile` or a tenant id.
    /// Tenants are validated one by one and committed concurrently; every
    /// tenant is attempted even when another fails. Returns one
    /// `(tenant_or_none, request_indices, IngestReport | None, error | None)`
    /// per tenant, in tenant order (tenants that could not be resolved after
    /// the others), then `(None, [index], None, error)` per unreadable
    /// request.
    ///
    /// `bodies` is a list of requests (for example the records of one queue
    /// batch): their rows are merged by tenant, so each tenant is written once.
    #[pyo3(signature = (bodies, *, contract, tenant_attribute, resolve, batch_id = None))]
    fn ingest_otel_logs<'py>(
        &self,
        py: Python<'py>,
        bodies: Vec<Vec<u8>>,
        contract: &str,
        tenant_attribute: &str,
        resolve: &Bound<'py, PyAny>,
        batch_id: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.parts.registry.get(contract)?;
        let flattened = py.detach(|| {
            let slices: Vec<&[u8]> = bodies.iter().map(Vec::as_slice).collect();
            crate::otel::flatten_logs_json_many(&slices, tenant_attribute)
        })?;
        let mut requests = Vec::with_capacity(flattened.tenants.len());
        // A tenant that cannot be resolved fails alone; the others go on.
        let mut unresolved = Vec::new();
        for group in flattened.tenants {
            let profile = match resolve
                .call1((group.tenant.clone(),))
                .and_then(|p| self.resolve(&p))
            {
                Ok(profile) => profile,
                Err(e) => {
                    unresolved.push(OtelOutcome {
                        tenant: group.tenant,
                        sources: group.sources,
                        report: Err(OtelError::Python(e)),
                    });
                    continue;
                }
            };
            let mut request =
                IngestRequest::new(profile, contract, RecordSource::Arrow(vec![group.batch]));
            if let Some(id) = &batch_id {
                request = request.with_batch_id(id.clone());
            }
            if let Some(hook) = self.hooks.get(contract) {
                request = request.with_hook(Arc::clone(hook));
            }
            requests.push((group.tenant, group.sources, request));
        }
        let malformed = flattened.malformed;
        let parts = Arc::clone(&self.parts);
        let cell = Arc::clone(&self.pipeline);
        future_into_py(py, async move {
            let pipeline = cell
                .get_or_try_init(|| async {
                    let storage = S3Storage::connect(parts.s3.clone()).await;
                    BronzePipeline::new(
                        Arc::clone(&parts.registry),
                        Arc::new(storage),
                        parts.config.clone(),
                    )
                    .map(Arc::new)
                })
                .await?;
            let worker = Arc::clone(pipeline);
            let prepared = tokio::task::spawn_blocking(move || {
                requests
                    .into_iter()
                    .map(|(tenant, sources, request)| (tenant, sources, worker.prepare(request)))
                    .collect::<Vec<_>>()
            })
            .await
            .map_err(|e| {
                DatalakeError::storage("runtime", format!("validation task failed: {e}"))
            })?;
            let mut commits = tokio::task::JoinSet::new();
            for (index, (tenant, sources, batch)) in prepared.into_iter().enumerate() {
                let pipeline = Arc::clone(pipeline);
                commits.spawn(async move {
                    let report = match batch {
                        Ok(batch) => pipeline.commit_with_retries(&batch, 4).await,
                        Err(e) => Err(e),
                    }
                    .map_err(OtelError::Datalake);
                    (
                        index,
                        OtelOutcome {
                            tenant,
                            sources,
                            report,
                        },
                    )
                });
            }
            let mut done = Vec::with_capacity(commits.len());
            while let Some(finished) = commits.join_next().await {
                done.push(finished.map_err(|e| {
                    DatalakeError::storage("runtime", format!("upload task failed: {e}"))
                })?);
            }
            done.sort_unstable_by_key(|(index, _)| *index);
            let mut outcomes: Vec<OtelOutcome> = done.into_iter().map(|(_, o)| o).collect();
            outcomes.extend(unresolved);
            outcomes.extend(malformed.into_iter().map(|(index, e)| OtelOutcome {
                tenant: None,
                sources: vec![index],
                report: Err(OtelError::Datalake(e)),
            }));
            Ok(outcomes)
        })
    }

    /// Ingests input of any size with bounded memory: validated in chunks,
    /// written as Parquet objects of about `target_object_mb` uploaded in
    /// parts while the next chunk is validated.
    ///
    /// `source` is an Arrow stream (any object with `__arrow_c_stream__`:
    /// a pyarrow `RecordBatchReader` or `Table`, a Polars data frame, a
    /// `DuckDB` result...), read batch by batch without the GIL; or an iterable
    /// of chunks, each one anything `ingest` accepts. With
    /// `parse_text_columns`, text columns of `integer`, `number` and
    /// `boolean` fields are parsed (CSV read as text). With `json_lines`,
    /// `str`/`bytes` chunks are JSON Lines. `max_object_seconds` closes an
    /// object once it is that old (checked as chunks arrive); `max_open_days`
    /// bounds the objects open at once when the contract partitions by event
    /// time.
    #[pyo3(signature = (
        source,
        *,
        tenant,
        contract,
        stream_id = None,
        target_object_mb = 256,
        part_mb = 16,
        row_group_mb = 64,
        chunk_rows = 131_072,
        max_inflight_parts = 4,
        parse_text_columns = false,
        json_lines = false,
        max_object_seconds = None,
        max_open_days = 4,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn ingest_stream<'py>(
        &self,
        py: Python<'py>,
        source: &Bound<'py, PyAny>,
        tenant: &Bound<'py, PyAny>,
        contract: &str,
        stream_id: Option<String>,
        target_object_mb: usize,
        part_mb: usize,
        row_group_mb: usize,
        chunk_rows: usize,
        max_inflight_parts: usize,
        parse_text_columns: bool,
        json_lines: bool,
        max_object_seconds: Option<f64>,
        max_open_days: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        const MIB: usize = 1024 * 1024;
        let max_object_age = max_object_seconds
            .map(|s| {
                std::time::Duration::try_from_secs_f64(s).map_err(|_| {
                    DatalakeError::config("max_object_seconds must be a non-negative number")
                })
            })
            .transpose()?;
        let tenant = self.resolve(tenant)?;
        self.parts.registry.get(contract)?;
        let options = StreamOptions {
            stream_id,
            target_object_bytes: target_object_mb.saturating_mul(MIB),
            part_bytes: part_mb.saturating_mul(MIB),
            row_group_bytes: row_group_mb.saturating_mul(MIB),
            chunk_rows,
            max_inflight_parts,
            max_object_age,
            max_open_days,
            ..StreamOptions::default()
        };
        let input = if source.hasattr("__arrow_c_stream__")? {
            StreamInput::Arrow(ArrowArrayStreamReader::from_pyarrow_bound(source)?)
        } else {
            StreamInput::Iter(source.try_iter()?.into_any().unbind())
        };
        let hook = self.hooks.get(contract).cloned();
        let contract = contract.to_owned();
        let parts = Arc::clone(&self.parts);
        let cell = Arc::clone(&self.pipeline);
        future_into_py(py, async move {
            let pipeline = cell
                .get_or_try_init(|| async {
                    let storage = S3Storage::connect(parts.s3.clone()).await;
                    BronzePipeline::new(
                        Arc::clone(&parts.registry),
                        Arc::new(storage),
                        parts.config.clone(),
                    )
                    .map(Arc::new)
                })
                .await?;
            let stream = pipeline.stream(tenant, &contract, hook, options)?;
            let report = tokio::task::spawn_blocking(move || {
                drive(stream, input, parse_text_columns, json_lines)
            })
            .await
            .map_err(|e| DatalakeError::storage("runtime", format!("stream task failed: {e}")))??;
            Ok(PyStreamReport::from(report))
        })
    }

    /// Tenant ids of the client's `tenants` registry (empty without one).
    #[getter]
    fn tenant_ids(&self) -> Vec<String> {
        self.parts
            .tenants
            .as_ref()
            .map(|t| t.tenant_ids().map(str::to_owned).collect())
            .unwrap_or_default()
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
            "BronzeIngestionClient(contracts={}, tenants={})",
            p.registry.len(),
            p.tenants.as_ref().map_or(0, |t| t.len())
        )
    }
}

impl PyBronzeIngestionClient {
    fn resolve(&self, tenant: &Bound<'_, PyAny>) -> PyResult<Arc<TenantProfile>> {
        if let Ok(profile) = tenant.cast::<PyTenantProfile>() {
            return Ok(Arc::clone(&profile.get().inner));
        }
        let Ok(tenant_id) = tenant.extract::<String>() else {
            return Err(PyTypeError::new_err(
                "tenant must be a TenantProfile or a tenant id string",
            ));
        };
        let registry = self.parts.tenants.as_ref().ok_or_else(|| {
            DatalakeError::config(
                "tenant given by id but the client has no tenants registry; \
                 pass a TenantProfile or create the client with tenants=TenantRegistry([...])",
            )
        })?;
        Ok(registry.resolve(&tenant_id)?)
    }
}

/// Feeds every chunk of `input` to `stream` and finishes it. Runs on a
/// blocking thread; takes the GIL only to pull Python chunks.
fn drive(
    mut stream: BronzeStream,
    input: StreamInput,
    parse_text_columns: bool,
    json_lines: bool,
) -> PyResult<StreamReport> {
    // Chunks are read on another thread, ahead of validation and encoding.
    let chunks: Box<dyn Iterator<Item = PyResult<RecordSource>> + Send> = match input {
        StreamInput::Arrow(reader) => Box::new(reader.map(move |batch| {
            let batch = batch.map_err(|e| DatalakeError::MalformedPayload {
                reason: format!("cannot read Arrow stream: {e}"),
            })?;
            Ok(if parse_text_columns {
                RecordSource::ArrowText(vec![batch])
            } else {
                RecordSource::Arrow(vec![batch])
            })
        })),
        StreamInput::Iter(iterator) => Box::new(std::iter::from_fn(move || {
            Python::attach(|py| next_chunk(py, iterator.bind(py), parse_text_columns, json_lines))
                .transpose()
        })),
    };
    for records in &crate::stream::prefetch(chunks) {
        stream.push(records?)?;
    }
    Ok(stream.finish()?)
}

/// Next chunk of a Python iterator, converted like an `ingest` batch.
fn next_chunk(
    py: Python<'_>,
    iterator: &Bound<'_, PyAny>,
    parse_text_columns: bool,
    json_lines: bool,
) -> PyResult<Option<RecordSource>> {
    let item = match iterator.call_method0("__next__") {
        Ok(item) => item,
        Err(e) if e.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) => return Ok(None),
        Err(e) => return Err(e),
    };
    if json_lines {
        if let Ok(text) = item.extract::<String>() {
            return Ok(Some(RecordSource::JsonLines(text)));
        }
        if let Ok(raw) = item.cast::<PyBytes>() {
            let text = String::from_utf8(raw.as_bytes().to_vec()).map_err(|e| {
                DatalakeError::MalformedPayload {
                    reason: format!("chunk is not UTF-8: {e}"),
                }
            })?;
            return Ok(Some(RecordSource::JsonLines(text)));
        }
    }
    Ok(Some(match to_source(py, &item)? {
        RecordSource::Arrow(batches) if parse_text_columns => RecordSource::ArrowText(batches),
        other => other,
    }))
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
