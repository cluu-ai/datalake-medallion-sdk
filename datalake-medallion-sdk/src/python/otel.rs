//! OpenTelemetry logs helpers.

use arrow::pyarrow::ToPyArrow;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::otel;

/// JSON text of the contract for flattened `OpenTelemetry` logs.
#[pyfunction]
#[pyo3(signature = (*, domain, datatype, schema_version, compression, partition_by_event_time))]
pub(crate) fn otel_logs_contract(
    domain: &str,
    datatype: &str,
    schema_version: &str,
    compression: &str,
    partition_by_event_time: bool,
) -> String {
    otel::logs_contract(
        domain,
        datatype,
        schema_version,
        compression,
        partition_by_event_time,
    )
    .to_string()
}

/// Reading options from the Python keyword arguments.
pub(crate) fn otlp_options(
    tenant_attribute: &str,
    encoding: &str,
    compression: &str,
    max_decompressed_bytes: usize,
) -> PyResult<otel::OtlpOptions> {
    Ok(otel::OtlpOptions::new(tenant_attribute)
        .with_encoding(encoding.parse()?)
        .with_compression(compression.parse()?)
        .with_max_decompressed_bytes(max_decompressed_bytes))
}

/// `"json"`, `"protobuf"` or `"mixed"` for the requests that were read.
pub(crate) fn encoding_summary(encodings: &[Option<otel::OtlpEncoding>]) -> Option<&'static str> {
    let mut read = encodings.iter().flatten();
    let first = *read.next()?;
    Some(if read.all(|&e| e == first) {
        first.as_str()
    } else {
        "mixed"
    })
}

/// Flattens an OTLP logs request (protobuf or JSON, optionally compressed)
/// into `(tenant, pyarrow.RecordBatch)` pairs, one per tenant, without
/// holding the GIL while decoding.
#[pyfunction]
#[pyo3(signature = (
    body,
    *,
    tenant_attribute,
    encoding = "auto",
    compression = "auto",
    // otel::DEFAULT_MAX_DECOMPRESSED_BYTES, as a literal for the Python signature.
    max_decompressed_bytes = 67_108_864,
))]
pub(crate) fn flatten_otlp_logs<'py>(
    py: Python<'py>,
    body: &Bound<'py, PyBytes>,
    tenant_attribute: &str,
    encoding: &str,
    compression: &str,
    max_decompressed_bytes: usize,
) -> PyResult<Vec<(Option<String>, Py<PyAny>)>> {
    let options = otlp_options(
        tenant_attribute,
        encoding,
        compression,
        max_decompressed_bytes,
    )?;
    let bytes = body.as_bytes();
    let groups = py.detach(|| otel::flatten_logs(bytes, &options))?;
    groups
        .into_iter()
        .map(|g| Ok((g.tenant, g.batch.to_pyarrow(py)?.unbind())))
        .collect()
}

/// Deterministic batch id (a ULID) for a set of source message ids at
/// `time_ms` milliseconds since the epoch.
#[pyfunction]
#[pyo3(signature = (message_ids, *, time_ms))]
pub(crate) fn batch_id_for(message_ids: Vec<String>, time_ms: i64) -> PyResult<String> {
    let time = chrono::DateTime::from_timestamp_millis(time_ms).ok_or_else(|| {
        crate::error::DatalakeError::config(format!("time_ms {time_ms} is out of range"))
    })?;
    Ok(crate::pipeline::batch_id_for(&message_ids, time))
}
