//! OpenTelemetry logs: OTLP/JSON export requests flattened into Arrow rows.
//!
//! An OTLP logs request nests records under resources and scopes
//! (`resourceLogs → scopeLogs → logRecords`). [`flatten_logs_json`] turns it
//! into one row per log record, with the columns of the
//! [OpenTelemetry Logs Data Model](https://opentelemetry.io/docs/specs/otel/logs/data-model/),
//! grouped by the tenant named in a resource attribute. The rows go straight
//! into the SDK's columnar path; [`logs_contract`] is the matching contract.
//!
//! Field names are read in the lowerCamelCase of the OTLP/JSON encoding,
//! and in the original `snake_case` as the specification allows. Trace and
//! span ids are hex text; 64-bit integers may be numbers or strings.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::Write as _;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Datelike, Timelike};
use serde::de::{self, Deserializer, Visitor};
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::error::DatalakeError;

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
    /// [`flatten_logs_json`]).
    pub sources: Vec<usize>,
}

/// Outcome of [`flatten_logs_json_many`].
#[derive(Debug)]
pub struct FlattenedRequests {
    /// Rows by tenant, merged across requests.
    pub tenants: Vec<TenantLogs>,
    /// Requests that could not be read, with their index and error; the
    /// other requests are flattened anyway.
    pub malformed: Vec<(usize, DatalakeError)>,
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

/// Unsigned nanoseconds since the epoch, as a number or a string.
#[derive(Debug, Clone, Copy)]
struct Nanos(Option<u64>);

impl<'de> Deserialize<'de> for Nanos {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Int::deserialize(d).map(|Int(v)| Nanos(v.and_then(|n| u64::try_from(n).ok())))
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
    double: Option<f64>,
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

/// JSON text of a string, escaped. Writing to a `Vec` cannot fail.
fn push_json_str(out: &mut Vec<u8>, s: &str) {
    let _ = serde_json::to_writer(&mut *out, s);
}

/// An OTLP `AnyValue` as plain JSON (bytes stay base64 text).
fn push_any(out: &mut Vec<u8>, v: &AnyValue<'_>) {
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
    } else if let Some(d) = v.double {
        let _ = serde_json::to_writer(&mut *out, &d);
    } else if let Some(a) = &v.array {
        out.push(b'[');
        for (n, item) in a.values.iter().enumerate() {
            if n > 0 {
                out.push(b',');
            }
            push_any(out, item);
        }
        out.push(b']');
    } else if let Some(kv) = &v.kvlist {
        push_key_values(out, &kv.values);
    } else if let Some(b) = &v.bytes {
        push_json_str(out, b);
    } else {
        out.extend_from_slice(b"null");
    }
}

fn push_key_values(out: &mut Vec<u8>, items: &[KeyValue<'_>]) {
    out.push(b'{');
    for (n, kv) in items.iter().enumerate() {
        if n > 0 {
            out.push(b',');
        }
        push_json_str(out, &kv.key);
        out.push(b':');
        match &kv.value {
            Some(v) => push_any(out, v),
            None => out.extend_from_slice(b"null"),
        }
    }
    out.push(b'}');
}

/// Attributes as JSON text in `out` (cleared first); `false` when empty.
fn attributes_json(out: &mut Vec<u8>, items: &[KeyValue<'_>]) -> bool {
    out.clear();
    if items.is_empty() {
        return false;
    }
    push_key_values(out, items);
    true
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
fn time_text(out: &mut String, nanos: Option<Nanos>) -> bool {
    out.clear();
    let Some(Nanos(Some(nanos))) = nanos else {
        return false;
    };
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

fn non_empty<'a>(s: Option<&'a Cow<'_, str>>) -> Option<&'a str> {
    s.map(AsRef::as_ref).filter(|s| !s.is_empty())
}

/// Lowercase hex id (OTLP/JSON ids are hex; some exporters use uppercase).
fn hex_id<'a>(s: Option<&'a Cow<'_, str>>, buf: &'a mut String) -> Option<&'a str> {
    let s = non_empty(s)?;
    if s.bytes().any(|b| b.is_ascii_uppercase()) {
        buf.clear();
        buf.extend(s.chars().map(|c| c.to_ascii_lowercase()));
        Some(buf.as_str())
    } else {
        Some(s)
    }
}

fn as_text(bytes: &[u8]) -> &str {
    // The JSON writers above only produce UTF-8.
    std::str::from_utf8(bytes).unwrap_or_default()
}

/// Flattens an OTLP/JSON logs export request into one row per log record,
/// grouped by the string value of the resource attribute `tenant_attribute`
/// (a record attribute of that name is used when the resource has none).
/// Groups come in tenant order, records in request order.
///
/// # Errors
///
/// [`DatalakeError::MalformedPayload`] when `body` is not a JSON object.
pub fn flatten_logs_json(
    body: &[u8],
    tenant_attribute: &str,
) -> Result<Vec<TenantLogs>, DatalakeError> {
    if body.iter().find(|b| !b.is_ascii_whitespace()) != Some(&b'{') {
        return Err(DatalakeError::MalformedPayload {
            reason: "OTLP logs request must be a JSON object".into(),
        });
    }
    let request: Request<'_> =
        serde_json::from_slice(body).map_err(|e| DatalakeError::MalformedPayload {
            reason: format!("OTLP logs request is not valid OTLP/JSON: {e}"),
        })?;
    // Few tenants per request: a linear search beats hashing owned keys.
    let mut groups: Vec<(Option<String>, Rows)> = Vec::new();
    let mut last: Option<usize> = None;
    let (mut time, mut observed, mut trace, mut span) =
        (String::new(), String::new(), String::new(), String::new());
    let (mut json, mut resource_json, mut body_json) = (Vec::new(), Vec::new(), Vec::new());
    for resource_logs in &request.resource_logs {
        let resource: &[KeyValue<'_>] = resource_logs
            .resource
            .as_ref()
            .map_or(&[], |r| r.attributes.as_slice());
        let resource_tenant = string_attribute(resource, tenant_attribute);
        let service = string_attribute(resource, "service.name");
        let has_resource = attributes_json(&mut resource_json, resource);
        for scope_logs in &resource_logs.scope_logs {
            let scope = scope_logs.scope.as_ref();
            let scope_name = scope.and_then(|s| non_empty(s.name.as_ref()));
            let scope_version = scope.and_then(|s| non_empty(s.version.as_ref()));
            for log in &scope_logs.log_records {
                let tenant =
                    resource_tenant.or_else(|| string_attribute(&log.attributes, tenant_attribute));
                let index = match last {
                    Some(i) if groups[i].0.as_deref() == tenant => i,
                    _ => {
                        let i = if let Some(i) =
                            groups.iter().position(|(t, _)| t.as_deref() == tenant)
                        {
                            i
                        } else {
                            groups.push((tenant.map(str::to_owned), Rows::new()));
                            groups.len() - 1
                        };
                        last = Some(i);
                        i
                    }
                };
                let rows = &mut groups[index].1;
                let has_observed = time_text(&mut observed, log.observed);
                // The record's time, or when it was observed if unset.
                if time_text(&mut time, log.time) {
                    rows.text[T_TIME].append_value(&time);
                } else if has_observed {
                    rows.text[T_TIME].append_value(&observed);
                } else {
                    rows.text[T_TIME].append_null();
                }
                rows.text[T_OBSERVED].append_option(has_observed.then_some(observed.as_str()));
                rows.text[T_SEVERITY].append_option(non_empty(log.severity_text.as_ref()));
                match &log.body {
                    Some(b) if b.string.is_some() => {
                        rows.text[T_BODY].append_option(b.string.as_deref());
                    }
                    Some(b) => {
                        body_json.clear();
                        push_any(&mut body_json, b);
                        let text = as_text(&body_json);
                        rows.text[T_BODY].append_option((text != "null").then_some(text));
                    }
                    None => rows.text[T_BODY].append_null(),
                }
                let has_attributes = attributes_json(&mut json, &log.attributes);
                rows.text[T_ATTRIBUTES].append_option(has_attributes.then(|| as_text(&json)));
                rows.text[T_TRACE].append_option(hex_id(log.trace_id.as_ref(), &mut trace));
                rows.text[T_SPAN].append_option(hex_id(log.span_id.as_ref(), &mut span));
                rows.text[T_EVENT].append_option(non_empty(log.event_name.as_ref()));
                rows.text[T_SERVICE].append_option(service);
                rows.text[T_SCOPE_NAME].append_option(scope_name);
                rows.text[T_SCOPE_VERSION].append_option(scope_version);
                rows.text[T_RESOURCE].append_option(has_resource.then(|| as_text(&resource_json)));
                rows.ints[I_SEVERITY].append_option(log.severity_number.and_then(|Int(v)| v));
                rows.ints[I_FLAGS].append_option(log.flags.and_then(|Int(v)| v));
            }
        }
    }
    groups.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    groups
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

/// Flattens several OTLP/JSON requests (for example the records of one
/// queue batch) and merges their rows by tenant, in request order, so each
/// tenant is ingested once.
///
/// # Errors
///
/// [`DatalakeError::Storage`] if merging Arrow batches fails; unreadable
/// requests are reported in [`FlattenedRequests::malformed`] instead.
pub fn flatten_logs_json_many(
    bodies: &[&[u8]],
    tenant_attribute: &str,
) -> Result<FlattenedRequests, DatalakeError> {
    use rayon::prelude::*;

    // Requests are independent: flatten them on every core, merge in order.
    let flattened: Vec<_> = bodies
        .par_iter()
        .map(|body| flatten_logs_json(body, tenant_attribute))
        .collect();
    let mut groups: BTreeMap<Option<String>, (Vec<RecordBatch>, Vec<usize>)> = BTreeMap::new();
    let mut malformed = Vec::new();
    for (index, result) in flattened.into_iter().enumerate() {
        match result {
            Ok(parts) => {
                for part in parts {
                    let (batches, sources) = groups.entry(part.tenant).or_default();
                    batches.push(part.batch);
                    sources.push(index);
                }
            }
            Err(e) => malformed.push((index, e)),
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
    Ok(FlattenedRequests { tenants, malformed })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::ContractRegistry;
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::Int64Type;

    fn request() -> Value {
        json!({"resourceLogs": [
            {"resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}},
                {"key": "tenant.id", "value": {"stringValue": "acme-001"}}]},
             "scopeLogs": [{"scope": {"name": "app", "version": "1.2"}, "logRecords": [
                {"timeUnixNano": "1772359200123456789", "observedTimeUnixNano": 1_772_359_200_200_000_000_u64,
                 "severityNumber": 9, "severityText": "INFO",
                 "body": {"stringValue": "order placed"},
                 "attributes": [{"key": "order.id", "value": {"intValue": "42"}},
                                {"key": "ok", "value": {"boolValue": true}}],
                 "traceId": "5B8EFFF798038103D269B633813FC60C", "spanId": "EEE19B7EC3C1B174", "flags": 1},
                {"observedTimeUnixNano": "1772359201000000000", "severityNumber": 17,
                 "body": {"kvlistValue": {"values": [{"key": "error", "value": {"stringValue": "boom"}}]}}}]}]},
            {"resource": {"attributes": [{"key": "tenant.id", "value": {"stringValue": "beta-002"}}]},
             "scope_logs": [{"log_records": [{"time_unix_nano": "1772359202000000000"}]}]},
            {"resource": {}, "scopeLogs": [{"logRecords": [
                {"timeUnixNano": "1772359203000000000",
                 "attributes": [{"key": "tenant.id", "value": {"stringValue": "acme-001"}}]},
                {"timeUnixNano": "1772359204000000000"}]}]}
        ]})
    }

    fn column<'a>(batch: &'a RecordBatch, name: &str) -> &'a ArrayRef {
        batch.column_by_name(name).unwrap()
    }

    #[test]
    fn records_are_flattened_and_grouped_by_tenant() {
        let groups = flatten_logs_json(request().to_string().as_bytes(), "tenant.id").unwrap();
        let tenants: Vec<_> = groups.iter().map(|g| g.tenant.as_deref()).collect();
        assert_eq!(tenants, [None, Some("acme-001"), Some("beta-002")]);
        let acme = &groups[1].batch;
        assert_eq!(
            acme.num_rows(),
            3,
            "two records plus one tagged on the record"
        );
        let ts = column(acme, "timestamp").as_string::<i32>();
        assert_eq!(ts.value(0), "2026-03-01T10:00:00.123456Z");
        assert_eq!(
            ts.value(1),
            "2026-03-01T10:00:01.000000Z",
            "falls back to observed time"
        );
        let body = column(acme, "body").as_string::<i32>();
        assert_eq!(body.value(0), "order placed");
        assert_eq!(body.value(1), r#"{"error":"boom"}"#);
        assert_eq!(
            column(acme, "attributes").as_string::<i32>().value(0),
            r#"{"order.id":42,"ok":true}"#
        );
        assert_eq!(
            column(acme, "trace_id").as_string::<i32>().value(0),
            "5b8efff798038103d269b633813fc60c"
        );
        assert_eq!(
            column(acme, "service_name").as_string::<i32>().value(0),
            "checkout"
        );
        assert_eq!(
            column(acme, "scope_version").as_string::<i32>().value(0),
            "1.2"
        );
        let severity = column(acme, "severity_number").as_primitive::<Int64Type>();
        assert_eq!((severity.value(0), severity.value(1)), (9, 17));
        assert!(column(acme, "severity_text").is_null(1));
        assert!(column(acme, "resource_attributes").is_null(2));
        assert_eq!(
            groups[2].batch.num_rows(),
            1,
            "snake_case names are read too"
        );
        assert_eq!(groups[0].batch.num_rows(), 1);
    }

    #[test]
    fn contract_matches_the_flattened_columns() {
        let mut registry = ContractRegistry::default();
        registry
            .register(&logs_contract("otel", "logs", "1.0.0", "zstd", true))
            .unwrap();
        let contract = registry.get("otel/logs/1.0.0").unwrap();
        let groups = flatten_logs_json(request().to_string().as_bytes(), "tenant.id").unwrap();
        let schema = groups[0].batch.schema();
        let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        let declared: Vec<_> = contract.fields().iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names.len(), declared.len());
        for name in names {
            assert!(declared.contains(&name), "{name} is not declared");
        }
        assert_eq!(contract.partition_by(), Some("timestamp"));
    }

    #[tokio::test]
    async fn flattened_logs_ingest_with_typed_timestamps() {
        use crate::pipeline::{BronzePipeline, IngestRequest, PipelineConfig, RecordSource};
        use crate::storage::InMemoryStorage;
        use crate::tenant::{TenantId, TenantProfile};
        use arrow::datatypes::TimeUnit;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let mut registry = ContractRegistry::default();
        registry
            .register(&logs_contract("otel", "logs", "1.0.0", "zstd", false))
            .unwrap();
        let storage = InMemoryStorage::new();
        let config = PipelineConfig {
            max_future_skew: None,
            ..PipelineConfig::default()
        };
        let pipeline =
            BronzePipeline::new(Arc::new(registry), Arc::new(storage.clone()), config).unwrap();
        let groups = flatten_logs_json(request().to_string().as_bytes(), "tenant.id").unwrap();
        let acme = groups
            .into_iter()
            .find(|g| g.tenant.as_deref() == Some("acme-001"))
            .unwrap();
        let tenant = Arc::new(
            TenantProfile::for_bucket(TenantId::new("acme-001").unwrap(), "lake")
                .unwrap()
                .build()
                .unwrap(),
        );
        let report = pipeline
            .ingest(IngestRequest::new(
                tenant,
                "otel/logs/1.0.0",
                RecordSource::Arrow(vec![acme.batch]),
            ))
            .await
            .unwrap();
        assert_eq!((report.records_ok, report.records_quarantined), (3, 0));
        let uri = report.data_uri.unwrap();
        let body = storage
            .get(uri.strip_prefix("s3://").unwrap())
            .unwrap()
            .body;
        let stored = ParquetRecordBatchReaderBuilder::try_new(body)
            .unwrap()
            .build()
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(
            stored.column_by_name("timestamp").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn several_requests_merge_by_tenant() {
        let one = request().to_string();
        let out = flatten_logs_json_many(&[one.as_bytes(), b"nope", one.as_bytes()], "tenant.id")
            .unwrap();
        let rows: Vec<_> = out.tenants.iter().map(|g| g.batch.num_rows()).collect();
        assert_eq!(rows, [2, 6, 2]);
        assert!(out.tenants.iter().all(|g| g.sources == [0, 2]));
        assert_eq!(out.malformed.len(), 1);
        assert_eq!(
            out.malformed[0].0, 1,
            "the unreadable request is reported, the rest kept"
        );
    }

    /// The contract must keep the columnar fast path: content keywords are
    /// annotations in draft 2020-12, but may be asserted by draft 7.
    #[test]
    fn contract_keeps_the_columnar_path() {
        let mut registry = ContractRegistry::default();
        registry
            .register(&logs_contract("otel", "logs", "1.0.0", "zstd", false))
            .unwrap();
        assert!(registry
            .get("otel/logs/1.0.0")
            .unwrap()
            .columnar()
            .is_some());
        let mut draft7 = logs_contract("otel", "logs7", "1.0.0", "zstd", false);
        draft7["$schema"] = json!("http://json-schema.org/draft-07/schema#");
        registry.register(&draft7).unwrap();
        assert!(registry
            .get("otel/logs7/1.0.0")
            .unwrap()
            .columnar()
            .is_none());
    }

    #[test]
    fn invalid_payloads_are_rejected() {
        assert!(flatten_logs_json(b"not json", "tenant.id").is_err());
        assert!(flatten_logs_json(b"[1, 2]", "tenant.id").is_err());
        assert!(flatten_logs_json(b"{}", "tenant.id").unwrap().is_empty());
    }
}
