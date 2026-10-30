//! OpenTelemetry logs: OTLP export requests flattened into Arrow rows.
//!
//! An OTLP logs request nests records under resources and scopes
//! (`resourceLogs → scopeLogs → logRecords`). [`flatten_logs`] turns it
//! into one row per log record, with the columns of the
//! [OpenTelemetry Logs Data Model](https://opentelemetry.io/docs/specs/otel/logs/data-model/),
//! grouped by the tenant named in a resource attribute. The rows go straight
//! into the SDK's columnar path; [`logs_contract`] is the matching contract.
//!
//! Both OTLP encodings give the same rows: OTLP/protobuf (a serialized
//! `ExportLogsServiceRequest`, what OTLP exporters and the Collector's S3
//! exporter write by default) and OTLP/JSON, each optionally gzip or zstd
//! compressed ([`OtlpOptions`]). In JSON, field names are read in the
//! lowerCamelCase of the encoding and in the original `snake_case` as the
//! specification allows, trace and span ids are hex text and 64-bit integers
//! may be numbers or strings.
//!
//! Values that protobuf cannot tell apart from "unset" are null in both
//! encodings: a zero time, severity number or flags; an empty severity text,
//! event name, scope name or version; an empty or all-zero trace or span id.

mod protobuf;
#[cfg(test)]
mod transcode;

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::{Read, Write as _};
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Datelike, Timelike};
use serde::de::{self, Deserializer, Visitor};
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::error::DatalakeError;

/// Deepest nesting of values (arrays and key-value lists inside attributes
/// or bodies) that a request may have, in either encoding. Deeper requests
/// are malformed.
pub const MAX_VALUE_DEPTH: usize = 24;

/// Default of [`OtlpOptions::max_decompressed_bytes`]: 64 MiB.
pub const DEFAULT_MAX_DECOMPRESSED_BYTES: usize = 64 << 20;

/// Encoding of an OTLP logs request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum OtlpEncoding {
    /// JSON when the first non-blank byte (after decompression) is `{`,
    /// protobuf otherwise.
    #[default]
    Auto,
    /// OTLP/JSON (`application/json`).
    Json,
    /// OTLP/protobuf (`application/x-protobuf`): a serialized
    /// `ExportLogsServiceRequest`.
    Protobuf,
}

impl OtlpEncoding {
    /// `"auto"`, `"json"` or `"protobuf"`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Json => "json",
            Self::Protobuf => "protobuf",
        }
    }
}

impl FromStr for OtlpEncoding {
    type Err = DatalakeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "auto" => Ok(Self::Auto),
            "json" => Ok(Self::Json),
            "protobuf" => Ok(Self::Protobuf),
            _ => Err(DatalakeError::config(format!(
                "OTLP encoding {s:?} is not one of 'auto', 'json', 'protobuf'"
            ))),
        }
    }
}

/// Compression of an OTLP logs request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum OtlpCompression {
    /// gzip or zstd when the request starts with their magic bytes,
    /// uncompressed otherwise.
    #[default]
    Auto,
    /// Uncompressed.
    None,
    /// gzip (one or more members), as `Content-Encoding: gzip`.
    Gzip,
    /// zstd (one or more frames).
    Zstd,
}

impl OtlpCompression {
    /// `"auto"`, `"none"`, `"gzip"` or `"zstd"`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::None => "none",
            Self::Gzip => "gzip",
            Self::Zstd => "zstd",
        }
    }
}

impl FromStr for OtlpCompression {
    type Err = DatalakeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "auto" => Ok(Self::Auto),
            "none" => Ok(Self::None),
            "gzip" => Ok(Self::Gzip),
            "zstd" => Ok(Self::Zstd),
            _ => Err(DatalakeError::config(format!(
                "OTLP compression {s:?} is not one of 'auto', 'none', 'gzip', 'zstd'"
            ))),
        }
    }
}

/// How [`flatten_logs`] reads a request.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct OtlpOptions {
    /// Attribute that names the tenant: on the resource, or on the record
    /// when the resource has none. Default `tenant.id`.
    pub tenant_attribute: String,
    /// Default [`OtlpEncoding::Auto`].
    pub encoding: OtlpEncoding,
    /// Default [`OtlpCompression::Auto`].
    pub compression: OtlpCompression,
    /// Largest size a compressed request may expand to; above it the request
    /// fails with [`DatalakeError::PayloadTooLarge`] (a guard against
    /// compression bombs). Default [`DEFAULT_MAX_DECOMPRESSED_BYTES`].
    pub max_decompressed_bytes: usize,
}

impl OtlpOptions {
    /// Defaults, with the tenant in `tenant_attribute`.
    pub fn new(tenant_attribute: impl Into<String>) -> Self {
        Self {
            tenant_attribute: tenant_attribute.into(),
            encoding: OtlpEncoding::Auto,
            compression: OtlpCompression::Auto,
            max_decompressed_bytes: DEFAULT_MAX_DECOMPRESSED_BYTES,
        }
    }

    /// Reads requests in `encoding`.
    #[must_use]
    pub fn with_encoding(mut self, encoding: OtlpEncoding) -> Self {
        self.encoding = encoding;
        self
    }

    /// Reads requests compressed with `compression`.
    #[must_use]
    pub fn with_compression(mut self, compression: OtlpCompression) -> Self {
        self.compression = compression;
        self
    }

    /// Limits the size of a decompressed request.
    #[must_use]
    pub fn with_max_decompressed_bytes(mut self, bytes: usize) -> Self {
        self.max_decompressed_bytes = bytes;
        self
    }

    /// Strict OTLP/JSON, uncompressed: what [`flatten_logs_json`] reads.
    fn json(tenant_attribute: &str) -> Self {
        Self::new(tenant_attribute)
            .with_encoding(OtlpEncoding::Json)
            .with_compression(OtlpCompression::None)
    }
}

impl Default for OtlpOptions {
    fn default() -> Self {
        Self::new("tenant.id")
    }
}

/// Column type of a flattened log record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Column {
    /// RFC 3339 text, stored as `timestamp(µs, UTC)` by the contract.
    Time,
    Text,
    /// JSON text (structured bodies and attribute maps).
    Json,
    Int,
}

/// Columns of a flattened log record, in order. The contract and the
/// flattener both come from this list.
const COLUMNS: [(&str, Column); 14] = [
    ("timestamp", Column::Time),
    ("observed_timestamp", Column::Time),
    ("severity_number", Column::Int),
    ("severity_text", Column::Text),
    ("body", Column::Text),
    ("attributes", Column::Json),
    ("trace_id", Column::Text),
    ("span_id", Column::Text),
    ("flags", Column::Int),
    ("event_name", Column::Text),
    ("service_name", Column::Text),
    ("scope_name", Column::Text),
    ("scope_version", Column::Text),
    ("resource_attributes", Column::Json),
];

/// Log records of one tenant.
#[derive(Debug, Clone)]
pub struct TenantLogs {
    /// Value of the tenant attribute, `None` when a resource has none.
    pub tenant: Option<String>,
    /// One row per log record, columns as in [`logs_contract`].
    pub batch: RecordBatch,
    /// Indices of the requests that contributed rows (always `[0]` for
    /// [`flatten_logs`]).
    pub sources: Vec<usize>,
}

/// Outcome of [`flatten_logs_many`].
#[derive(Debug)]
pub struct FlattenedRequests {
    /// Rows by tenant, merged across requests.
    pub tenants: Vec<TenantLogs>,
    /// Requests that could not be read, with their index and error; the
    /// other requests are flattened anyway.
    pub malformed: Vec<(usize, DatalakeError)>,
    /// Encoding each request was read in (`None` for unreadable ones).
    pub encodings: Vec<Option<OtlpEncoding>>,
}

/// JSON Schema contract for flattened `OpenTelemetry` logs: event time typed as a
/// timestamp, zstd unless `compression` says otherwise, and event-day
/// partitions only when `partition_by_event_time` (see the README guidance).
#[must_use]
pub fn logs_contract(
    domain: &str,
    datatype: &str,
    schema_version: &str,
    compression: &str,
    partition_by_event_time: bool,
) -> Value {
    let mut properties = Map::new();
    for (name, column) in COLUMNS {
        let property = match (name, column) {
            (_, Column::Time) => {
                json!({"type": "string", "format": "date-time", "x-dl-type": "timestamp"})
            }
            (_, Column::Int) => json!({"type": "integer"}),
            (_, Column::Json) => json!({"type": "string", "contentMediaType": "application/json"}),
            ("trace_id", _) => json!({"type": "string", "pattern": "^[0-9a-f]{32}$"}),
            ("span_id", _) => json!({"type": "string", "pattern": "^[0-9a-f]{16}$"}),
            _ => json!({"type": "string"}),
        };
        properties.insert(name.to_owned(), property);
    }
    let mut contract = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "x-dl-domain": domain,
        "x-dl-datatype": datatype,
        "x-dl-schema_version": schema_version,
        "x-dl-compression": compression,
        "required": ["timestamp"],
        "properties": properties,
    });
    if partition_by_event_time {
        contract["x-dl-partition_by"] = json!("timestamp");
    }
    contract
}

/// Builders of one tenant's rows.
struct Rows {
    text: Vec<StringBuilder>,
    ints: Vec<Int64Builder>,
}

impl Rows {
    fn new() -> Self {
        let count = |kind: fn(Column) -> bool| COLUMNS.iter().filter(|(_, c)| kind(*c)).count();
        Self {
            text: (0..count(|c| c != Column::Int))
                .map(|_| StringBuilder::new())
                .collect(),
            ints: (0..count(|c| c == Column::Int))
                .map(|_| Int64Builder::new())
                .collect(),
        }
    }

    fn finish(mut self) -> Result<RecordBatch, DatalakeError> {
        let (mut t, mut i) = (0, 0);
        let mut fields = Vec::with_capacity(COLUMNS.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(COLUMNS.len());
        for (name, column) in COLUMNS {
            if column == Column::Int {
                fields.push(Field::new(name, DataType::Int64, true));
                arrays.push(Arc::new(self.ints[i].finish()));
                i += 1;
            } else {
                fields.push(Field::new(name, DataType::Utf8, true));
                arrays.push(Arc::new(self.text[t].finish()));
                t += 1;
            }
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|e| DatalakeError::storage("arrow", e.to_string()))
    }
}

// Positions of the text and integer builders, in [`COLUMNS`] order.
const T_TIME: usize = 0;
const T_OBSERVED: usize = 1;
const T_SEVERITY: usize = 2;
const T_BODY: usize = 3;
const T_ATTRIBUTES: usize = 4;
const T_TRACE: usize = 5;
const T_SPAN: usize = 6;
const T_EVENT: usize = 7;
const T_SERVICE: usize = 8;
const T_SCOPE_NAME: usize = 9;
const T_SCOPE_VERSION: usize = 10;
const T_RESOURCE: usize = 11;
const I_SEVERITY: usize = 0;
const I_FLAGS: usize = 1;

/// Resource and scope of the records being read.
struct Origin<'a> {
    service: Option<&'a str>,
    /// Resource attributes as JSON text.
    resource: Option<&'a str>,
    scope_name: Option<&'a str>,
    scope_version: Option<&'a str>,
}

/// One log record as read from either encoding; zero and empty mean unset.
#[derive(Default)]
struct Record<'a> {
    time: u64,
    observed: u64,
    severity_number: i64,
    severity_text: &'a str,
    /// Text, or JSON text for structured bodies.
    body: Option<&'a str>,
    /// Attributes as JSON text.
    attributes: Option<&'a str>,
    /// Lowercase hex.
    trace_id: Option<&'a str>,
    span_id: Option<&'a str>,
    flags: i64,
    event_name: &'a str,
}

/// Rows of a request by tenant, in order of first appearance.
#[derive(Default)]
struct Groups {
    tenants: Vec<(Option<String>, Rows)>,
    last: Option<usize>,
    time: String,
    observed: String,
}

impl Groups {
    fn index(&mut self, tenant: Option<&str>) -> usize {
        // Few tenants per request: a linear search beats hashing owned keys.
        if let Some(i) = self
            .last
            .filter(|&i| self.tenants[i].0.as_deref() == tenant)
        {
            return i;
        }
        let i = if let Some(i) = self
            .tenants
            .iter()
            .position(|(t, _)| t.as_deref() == tenant)
        {
            i
        } else {
            self.tenants.push((tenant.map(str::to_owned), Rows::new()));
            self.tenants.len() - 1
        };
        self.last = Some(i);
        i
    }

    fn push(&mut self, tenant: Option<&str>, origin: &Origin<'_>, record: &Record<'_>) {
        let index = self.index(tenant);
        let rows = &mut self.tenants[index].1;
        let has_observed = time_text(&mut self.observed, record.observed);
        // The record's time, or when it was observed if unset.
        if time_text(&mut self.time, record.time) {
            rows.text[T_TIME].append_value(&self.time);
        } else if has_observed {
            rows.text[T_TIME].append_value(&self.observed);
        } else {
            rows.text[T_TIME].append_null();
        }
        rows.text[T_OBSERVED].append_option(has_observed.then_some(self.observed.as_str()));
        rows.text[T_SEVERITY].append_option(non_empty(record.severity_text));
        rows.text[T_BODY].append_option(record.body);
        rows.text[T_ATTRIBUTES].append_option(record.attributes);
        rows.text[T_TRACE].append_option(record.trace_id);
        rows.text[T_SPAN].append_option(record.span_id);
        rows.text[T_EVENT].append_option(non_empty(record.event_name));
        rows.text[T_SERVICE].append_option(origin.service);
        rows.text[T_SCOPE_NAME].append_option(origin.scope_name);
        rows.text[T_SCOPE_VERSION].append_option(origin.scope_version);
        rows.text[T_RESOURCE].append_option(origin.resource);
        rows.ints[I_SEVERITY]
            .append_option((record.severity_number != 0).then_some(record.severity_number));
        rows.ints[I_FLAGS].append_option((record.flags != 0).then_some(record.flags));
    }

    /// Groups in tenant order.
    fn finish(mut self) -> Result<Vec<TenantLogs>, DatalakeError> {
        self.tenants.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        self.tenants
            .into_iter()
            .map(|(tenant, rows)| {
                Ok(TenantLogs {
                    tenant,
                    batch: rows.finish()?,
                    sources: vec![0],
                })
            })
            .collect()
    }
}

/// A 64-bit integer written as a JSON number or a string, as OTLP/JSON
/// allows; anything else reads as absent.
#[derive(Debug, Clone, Copy)]
struct Int(Option<i64>);

impl<'de> Deserialize<'de> for Int {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct IntVisitor;
        impl Visitor<'_> for IntVisitor {
            type Value = Int;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("an integer, or a string of digits")
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Int, E> {
                Ok(Int(Some(v)))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Int, E> {
                Ok(Int(i64::try_from(v).ok()))
            }
            fn visit_f64<E: de::Error>(self, _: f64) -> Result<Int, E> {
                Ok(Int(None))
            }
            fn visit_bool<E: de::Error>(self, _: bool) -> Result<Int, E> {
                Ok(Int(None))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Int, E> {
                Ok(Int(v.parse().ok()))
            }
        }
        d.deserialize_any(IntVisitor)
    }
}

fn int(v: Option<Int>) -> i64 {
    v.and_then(|Int(v)| v).unwrap_or(0)
}

/// Unsigned nanoseconds since the epoch, as a number or a string.
#[derive(Debug, Clone, Copy)]
struct Nanos(Option<u64>);

impl<'de> Deserialize<'de> for Nanos {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Int::deserialize(d).map(|Int(v)| Nanos(v.and_then(|n| u64::try_from(n).ok())))
    }
}

fn nanos(v: Option<Nanos>) -> u64 {
    v.and_then(|Nanos(v)| v).unwrap_or(0)
}

/// A double written as a JSON number, or as a string (`"NaN"`, `"Infinity"`,
/// `"-Infinity"` or digits) as the protobuf JSON mapping allows.
#[derive(Debug, Clone, Copy)]
struct Double(f64);

impl<'de> Deserialize<'de> for Double {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct DoubleVisitor;
        impl Visitor<'_> for DoubleVisitor {
            type Value = Double;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a number, or a string")
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Double, E> {
                Ok(Double(v))
            }
            #[allow(clippy::cast_precision_loss)] // As JSON number parsing does.
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Double, E> {
                Ok(Double(v as f64))
            }
            #[allow(clippy::cast_precision_loss)]
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Double, E> {
                Ok(Double(v as f64))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Double, E> {
                Ok(Double(match v {
                    "Infinity" => f64::INFINITY,
                    "-Infinity" => f64::NEG_INFINITY,
                    _ => v.parse().unwrap_or(f64::NAN),
                }))
            }
        }
        d.deserialize_any(DoubleVisitor)
    }
}

#[derive(Deserialize, Default)]
struct Request<'a> {
    #[serde(borrow, default, rename = "resourceLogs", alias = "resource_logs")]
    resource_logs: Vec<ResourceLogs<'a>>,
}

#[derive(Deserialize, Default)]
struct ResourceLogs<'a> {
    #[serde(borrow, default)]
    resource: Option<Attributes<'a>>,
    #[serde(borrow, default, rename = "scopeLogs", alias = "scope_logs")]
    scope_logs: Vec<ScopeLogs<'a>>,
}

#[derive(Deserialize, Default)]
struct Attributes<'a> {
    #[serde(borrow, default)]
    attributes: Vec<KeyValue<'a>>,
}

#[derive(Deserialize, Default)]
struct ScopeLogs<'a> {
    #[serde(borrow, default)]
    scope: Option<Scope<'a>>,
    #[serde(borrow, default, rename = "logRecords", alias = "log_records")]
    log_records: Vec<LogRecord<'a>>,
}

#[derive(Deserialize, Default)]
struct Scope<'a> {
    #[serde(borrow, default)]
    name: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    version: Option<Cow<'a, str>>,
}

#[derive(Deserialize, Default)]
struct LogRecord<'a> {
    #[serde(default, rename = "timeUnixNano", alias = "time_unix_nano")]
    time: Option<Nanos>,
    #[serde(
        default,
        rename = "observedTimeUnixNano",
        alias = "observed_time_unix_nano"
    )]
    observed: Option<Nanos>,
    #[serde(default, rename = "severityNumber", alias = "severity_number")]
    severity_number: Option<Int>,
    #[serde(borrow, default, rename = "severityText", alias = "severity_text")]
    severity_text: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    body: Option<AnyValue<'a>>,
    #[serde(borrow, default)]
    attributes: Vec<KeyValue<'a>>,
    #[serde(borrow, default, rename = "traceId", alias = "trace_id")]
    trace_id: Option<Cow<'a, str>>,
    #[serde(borrow, default, rename = "spanId", alias = "span_id")]
    span_id: Option<Cow<'a, str>>,
    #[serde(default)]
    flags: Option<Int>,
    #[serde(borrow, default, rename = "eventName", alias = "event_name")]
    event_name: Option<Cow<'a, str>>,
}

#[derive(Deserialize)]
struct KeyValue<'a> {
    #[serde(borrow)]
    key: Cow<'a, str>,
    #[serde(borrow, default)]
    value: Option<AnyValue<'a>>,
}

#[derive(Deserialize, Default)]
struct AnyValue<'a> {
    #[serde(borrow, default, rename = "stringValue", alias = "string_value")]
    string: Option<Cow<'a, str>>,
    #[serde(default, rename = "boolValue", alias = "bool_value")]
    boolean: Option<bool>,
    #[serde(default, rename = "intValue", alias = "int_value")]
    int: Option<Int>,
    #[serde(default, rename = "doubleValue", alias = "double_value")]
    double: Option<Double>,
    #[serde(borrow, default, rename = "arrayValue", alias = "array_value")]
    array: Option<Values<'a, AnyValue<'a>>>,
    #[serde(borrow, default, rename = "kvlistValue", alias = "kvlist_value")]
    kvlist: Option<Values<'a, KeyValue<'a>>>,
    #[serde(borrow, default, rename = "bytesValue", alias = "bytes_value")]
    bytes: Option<Cow<'a, str>>,
}

#[derive(Deserialize)]
#[serde(bound(deserialize = "T: Deserialize<'de>"))]
struct Values<'a, T> {
    #[serde(default = "Vec::new")]
    values: Vec<T>,
    #[serde(skip)]
    _borrow: std::marker::PhantomData<&'a ()>,
}

fn malformed(reason: impl Into<String>) -> DatalakeError {
    DatalakeError::MalformedPayload {
        reason: reason.into(),
    }
}

fn too_deep() -> DatalakeError {
    malformed(format!(
        "OTLP logs request nests values deeper than {MAX_VALUE_DEPTH} levels"
    ))
}

/// JSON text of a string, escaped. Writing to a `Vec` cannot fail.
fn push_json_str(out: &mut Vec<u8>, s: &str) {
    let _ = serde_json::to_writer(&mut *out, s);
}

/// JSON text of a double; non-finite values are `null`.
fn push_double(out: &mut Vec<u8>, d: f64) {
    let _ = serde_json::to_writer(&mut *out, &d);
}

/// An OTLP `AnyValue` at nesting `depth` as plain JSON (bytes stay base64 text).
fn push_any(out: &mut Vec<u8>, v: &AnyValue<'_>, depth: usize) -> Result<(), DatalakeError> {
    if depth > MAX_VALUE_DEPTH {
        return Err(too_deep());
    }
    if let Some(s) = &v.string {
        push_json_str(out, s);
    } else if let Some(b) = v.boolean {
        out.extend_from_slice(if b { b"true" } else { b"false" });
    } else if let Some(Int(i)) = v.int {
        match i {
            Some(i) => {
                let _ = write!(out, "{i}");
            }
            None => out.extend_from_slice(b"null"),
        }
    } else if let Some(Double(d)) = v.double {
        push_double(out, d);
    } else if let Some(a) = &v.array {
        out.push(b'[');
        for (n, item) in a.values.iter().enumerate() {
            if n > 0 {
                out.push(b',');
            }
            push_any(out, item, depth + 1)?;
        }
        out.push(b']');
    } else if let Some(kv) = &v.kvlist {
        push_key_values(out, &kv.values, depth + 1)?;
    } else if let Some(b) = &v.bytes {
        push_json_str(out, b);
    } else {
        out.extend_from_slice(b"null");
    }
    Ok(())
}

fn push_key_values(
    out: &mut Vec<u8>,
    items: &[KeyValue<'_>],
    depth: usize,
) -> Result<(), DatalakeError> {
    out.push(b'{');
    for (n, kv) in items.iter().enumerate() {
        if n > 0 {
            out.push(b',');
        }
        push_json_str(out, &kv.key);
        out.push(b':');
        match &kv.value {
            Some(v) => push_any(out, v, depth)?,
            None => out.extend_from_slice(b"null"),
        }
    }
    out.push(b'}');
    Ok(())
}

/// Attributes as JSON text in `out` (cleared first); `false` when empty.
fn attributes_json(out: &mut Vec<u8>, items: &[KeyValue<'_>]) -> Result<bool, DatalakeError> {
    out.clear();
    if items.is_empty() {
        return Ok(false);
    }
    push_key_values(out, items, 1)?;
    Ok(true)
}

/// The string value of attribute `key`.
fn string_attribute<'a>(items: &'a [KeyValue<'_>], key: &str) -> Option<&'a str> {
    items
        .iter()
        .find(|kv| kv.key == key)
        .and_then(|kv| kv.value.as_ref()?.string.as_deref())
}

/// Nanoseconds as RFC 3339 text with microseconds in `out` (cleared first);
/// `false` for zero (unset) or out of range.
fn time_text(out: &mut String, nanos: u64) -> bool {
    out.clear();
    if nanos == 0 {
        return false;
    }
    let (Ok(secs), Ok(sub)) = (
        i64::try_from(nanos / 1_000_000_000),
        u32::try_from(nanos % 1_000_000_000),
    ) else {
        return false;
    };
    let Some(t) = DateTime::from_timestamp(secs, sub) else {
        return false;
    };
    // Written field by field: much cheaper than interpreting a format
    // string twice per record.
    let _ = write!(
        out,
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}Z",
        t.year(),
        t.month(),
        t.day(),
        t.hour(),
        t.minute(),
        t.second(),
        t.timestamp_subsec_micros()
    );
    true
}

fn non_empty(s: &str) -> Option<&str> {
    (!s.is_empty()).then_some(s)
}

/// Lowercase hex id (OTLP/JSON ids are hex; some exporters use uppercase);
/// `None` when empty or all zeros (an invalid id).
fn hex_id<'a>(s: Option<&'a str>, buf: &'a mut String) -> Option<&'a str> {
    let s = s.filter(|s| s.bytes().any(|b| b != b'0'))?;
    if s.bytes().any(|b| b.is_ascii_uppercase()) {
        buf.clear();
        buf.extend(s.chars().map(|c| c.to_ascii_lowercase()));
        Some(buf.as_str())
    } else {
        Some(s)
    }
}

/// Lowercase hex of a binary id; `None` when empty or all zeros.
fn hex_byte_id<'a>(id: &[u8], buf: &'a mut String) -> Option<&'a str> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    if id.iter().all(|&b| b == 0) {
        return None;
    }
    buf.clear();
    for &b in id {
        buf.push(char::from(HEX[usize::from(b >> 4)]));
        buf.push(char::from(HEX[usize::from(b & 0x0f)]));
    }
    Some(buf.as_str())
}

fn as_text(bytes: &[u8]) -> &str {
    // The JSON writers above only produce UTF-8.
    std::str::from_utf8(bytes).unwrap_or_default()
}

fn first_non_blank(data: &[u8]) -> Option<u8> {
    data.iter().copied().find(|b| !b.is_ascii_whitespace())
}

/// Reads an OTLP/JSON request into `groups`.
fn read_json(
    body: &[u8],
    tenant_attribute: &str,
    groups: &mut Groups,
) -> Result<(), DatalakeError> {
    if first_non_blank(body) != Some(b'{') {
        return Err(malformed("OTLP logs request must be a JSON object"));
    }
    let request: Request<'_> = serde_json::from_slice(body)
        .map_err(|e| malformed(format!("OTLP logs request is not valid OTLP/JSON: {e}")))?;
    let (mut trace, mut span) = (String::new(), String::new());
    let (mut json, mut resource_json, mut body_json) = (Vec::new(), Vec::new(), Vec::new());
    for resource_logs in &request.resource_logs {
        let resource: &[KeyValue<'_>] = resource_logs
            .resource
            .as_ref()
            .map_or(&[], |r| r.attributes.as_slice());
        let resource_tenant = string_attribute(resource, tenant_attribute);
        let has_resource = attributes_json(&mut resource_json, resource)?;
        for scope_logs in &resource_logs.scope_logs {
            let scope = scope_logs.scope.as_ref();
            let origin = Origin {
                service: string_attribute(resource, "service.name"),
                resource: has_resource.then(|| as_text(&resource_json)),
                scope_name: scope.and_then(|s| s.name.as_deref()).and_then(non_empty),
                scope_version: scope.and_then(|s| s.version.as_deref()).and_then(non_empty),
            };
            for log in &scope_logs.log_records {
                let tenant =
                    resource_tenant.or_else(|| string_attribute(&log.attributes, tenant_attribute));
                let body = match &log.body {
                    Some(b) if b.string.is_some() => b.string.as_deref(),
                    Some(b) => {
                        body_json.clear();
                        push_any(&mut body_json, b, 1)?;
                        let text = as_text(&body_json);
                        (text != "null").then_some(text)
                    }
                    None => None,
                };
                let has_attributes = attributes_json(&mut json, &log.attributes)?;
                let record = Record {
                    time: nanos(log.time),
                    observed: nanos(log.observed),
                    severity_number: int(log.severity_number),
                    severity_text: log.severity_text.as_deref().unwrap_or_default(),
                    body,
                    attributes: has_attributes.then(|| as_text(&json)),
                    trace_id: hex_id(log.trace_id.as_deref(), &mut trace),
                    span_id: hex_id(log.span_id.as_deref(), &mut span),
                    flags: int(log.flags),
                    event_name: log.event_name.as_deref().unwrap_or_default(),
                };
                groups.push(tenant, &origin, &record);
            }
        }
    }
    Ok(())
}

const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];

/// The request decompressed, at most `limit` bytes.
fn decompress(
    body: &[u8],
    compression: OtlpCompression,
    limit: usize,
) -> Result<Cow<'_, [u8]>, DatalakeError> {
    let compression = match compression {
        OtlpCompression::Auto if body.starts_with(&GZIP_MAGIC) => OtlpCompression::Gzip,
        OtlpCompression::Auto if body.starts_with(&ZSTD_MAGIC) => OtlpCompression::Zstd,
        OtlpCompression::Auto => OtlpCompression::None,
        c => c,
    };
    // Start from the size the stream declares, bounded so that a forged size
    // cannot reserve much memory.
    let capacity = |declared: Option<u64>| {
        declared
            .and_then(|n| usize::try_from(n).ok())
            .unwrap_or(body.len().saturating_mul(4))
            .min(limit)
            .min(body.len().saturating_mul(64))
    };
    match compression {
        OtlpCompression::Gzip => {
            // ISIZE: the last member's size modulo 2^32.
            let declared = body
                .len()
                .checked_sub(4)
                .and_then(|start| body.get(start..))
                .and_then(|tail| <[u8; 4]>::try_from(tail).ok())
                .map(|size| u64::from(u32::from_le_bytes(size)));
            read_limited(
                flate2::read::MultiGzDecoder::new(body),
                capacity(declared),
                limit,
                "gzip",
            )
        }
        OtlpCompression::Zstd => {
            let declared = zstd::zstd_safe::get_frame_content_size(body).ok().flatten();
            let decoder = zstd::stream::read::Decoder::with_buffer(body)
                .map_err(|e| malformed(format!("cannot decompress the zstd OTLP request: {e}")))?;
            read_limited(decoder, capacity(declared), limit, "zstd")
        }
        OtlpCompression::Auto | OtlpCompression::None => Ok(Cow::Borrowed(body)),
    }
}

fn read_limited(
    reader: impl Read,
    capacity: usize,
    limit: usize,
    codec: &str,
) -> Result<Cow<'static, [u8]>, DatalakeError> {
    let mut out = Vec::with_capacity(capacity);
    let cap = u64::try_from(limit).unwrap_or(u64::MAX).saturating_add(1);
    reader
        .take(cap)
        .read_to_end(&mut out)
        .map_err(|e| malformed(format!("cannot decompress the {codec} OTLP request: {e}")))?;
    if out.len() > limit {
        return Err(DatalakeError::PayloadTooLarge { limit });
    }
    Ok(Cow::Owned(out))
}

/// One request: its groups and the encoding it was read in.
fn flatten_request(
    body: &[u8],
    options: &OtlpOptions,
) -> Result<(Vec<TenantLogs>, OtlpEncoding), DatalakeError> {
    let data = decompress(body, options.compression, options.max_decompressed_bytes)?;
    let encoding = match options.encoding {
        OtlpEncoding::Auto if first_non_blank(&data) == Some(b'{') => OtlpEncoding::Json,
        OtlpEncoding::Auto => OtlpEncoding::Protobuf,
        e => e,
    };
    let mut groups = Groups::default();
    if encoding == OtlpEncoding::Json {
        read_json(&data, &options.tenant_attribute, &mut groups)?;
    } else {
        protobuf::read(&data, &options.tenant_attribute, &mut groups)?;
    }
    Ok((groups.finish()?, encoding))
}

/// Flattens an OTLP logs export request (protobuf or JSON, optionally
/// compressed, as `options` says) into one row per log record, grouped by
/// the string value of the resource attribute `options.tenant_attribute`
/// (a record attribute of that name is used when the resource has none).
/// Groups come in tenant order, records in request order.
///
/// # Errors
///
/// [`DatalakeError::MalformedPayload`] when `body` cannot be decompressed or
/// is not a valid request in its encoding, or nests values deeper than
/// [`MAX_VALUE_DEPTH`]; [`DatalakeError::PayloadTooLarge`] when it expands
/// beyond `options.max_decompressed_bytes`.
pub fn flatten_logs(body: &[u8], options: &OtlpOptions) -> Result<Vec<TenantLogs>, DatalakeError> {
    flatten_request(body, options).map(|(groups, _)| groups)
}

/// [`flatten_logs`] for an uncompressed OTLP/JSON request.
///
/// # Errors
///
/// [`DatalakeError::MalformedPayload`] when `body` is not a JSON object.
pub fn flatten_logs_json(
    body: &[u8],
    tenant_attribute: &str,
) -> Result<Vec<TenantLogs>, DatalakeError> {
    flatten_logs(body, &OtlpOptions::json(tenant_attribute))
}

/// Flattens several requests (for example the records of one queue batch;
/// each may have its own encoding and compression when `options` says
/// `Auto`) and merges their rows by tenant, in request order, so each tenant
/// is ingested once.
///
/// # Errors
///
/// [`DatalakeError::Storage`] if merging Arrow batches fails; unreadable
/// requests are reported in [`FlattenedRequests::malformed`] instead.
pub fn flatten_logs_many(
    bodies: &[&[u8]],
    options: &OtlpOptions,
) -> Result<FlattenedRequests, DatalakeError> {
    use rayon::prelude::*;

    // Requests are independent: flatten them on every core, merge in order.
    let flattened: Vec<_> = bodies
        .par_iter()
        .map(|body| flatten_request(body, options))
        .collect();
    let mut groups: BTreeMap<Option<String>, (Vec<RecordBatch>, Vec<usize>)> = BTreeMap::new();
    let mut malformed = Vec::new();
    let mut encodings = Vec::with_capacity(bodies.len());
    for (index, result) in flattened.into_iter().enumerate() {
        match result {
            Ok((parts, encoding)) => {
                encodings.push(Some(encoding));
                for part in parts {
                    let (batches, sources) = groups.entry(part.tenant).or_default();
                    batches.push(part.batch);
                    sources.push(index);
                }
            }
            Err(e) => {
                encodings.push(None);
                malformed.push((index, e));
            }
        }
    }
    let tenants = groups
        .into_iter()
        .map(|(tenant, (batches, sources))| {
            let batch = if batches.len() == 1 {
                batches.into_iter().next().unwrap_or_else(|| unreachable!())
            } else {
                arrow::compute::concat_batches(&batches[0].schema(), &batches)
                    .map_err(|e| DatalakeError::storage("arrow", e.to_string()))?
            };
            Ok(TenantLogs {
                tenant,
                batch,
                sources,
            })
        })
        .collect::<Result<_, DatalakeError>>()?;
    Ok(FlattenedRequests {
        tenants,
        malformed,
        encodings,
    })
}

/// [`flatten_logs_many`] for uncompressed OTLP/JSON requests.
///
/// # Errors
///
/// As [`flatten_logs_many`].
pub fn flatten_logs_json_many(
    bodies: &[&[u8]],
    tenant_attribute: &str,
) -> Result<FlattenedRequests, DatalakeError> {
    flatten_logs_many(bodies, &OtlpOptions::json(tenant_attribute))
}

#[cfg(test)]
mod tests;
