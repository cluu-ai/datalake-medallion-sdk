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

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, SecondsFormat};
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

/// One record's values, in [`COLUMNS`] order.
#[derive(Default)]
struct Record {
    text: Vec<Option<String>>,
    ints: Vec<Option<i64>>,
}

/// `obj[camel]`, or `obj[snake]` as the specification allows.
fn get<'a>(obj: &'a Value, camel: &str, snake: &str) -> Option<&'a Value> {
    obj.get(camel).or_else(|| obj.get(snake))
}

fn array<'a>(obj: &'a Value, camel: &str, snake: &str) -> &'a [Value] {
    get(obj, camel, snake)
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice)
}

/// A 64-bit integer written as a number or as a string.
fn int(v: Option<&Value>) -> Option<i64> {
    match v? {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_u64().and_then(|u| i64::try_from(u).ok())),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Nanoseconds since the epoch as RFC 3339 text with microseconds; `None`
/// for zero (unset) or out of range.
fn time(v: Option<&Value>) -> Option<String> {
    let nanos = match v? {
        Value::Number(n) => n.as_u64()?,
        Value::String(s) => s.parse().ok()?,
        _ => return None,
    };
    if nanos == 0 {
        return None;
    }
    let secs = i64::try_from(nanos / 1_000_000_000).ok()?;
    let sub = u32::try_from(nanos % 1_000_000_000).ok()?;
    DateTime::from_timestamp(secs, sub).map(|t| t.to_rfc3339_opts(SecondsFormat::Micros, true))
}

/// An OTLP `AnyValue` as plain JSON.
fn any_value(v: &Value) -> Value {
    if let Some(s) = get(v, "stringValue", "string_value") {
        return s.clone();
    }
    if let Some(b) = get(v, "boolValue", "bool_value") {
        return b.clone();
    }
    if let Some(i) = get(v, "intValue", "int_value") {
        return int(Some(i)).map_or(Value::Null, Value::from);
    }
    if let Some(d) = get(v, "doubleValue", "double_value") {
        return d.clone();
    }
    if let Some(a) = get(v, "arrayValue", "array_value") {
        return Value::Array(array(a, "values", "values").iter().map(any_value).collect());
    }
    if let Some(kv) = get(v, "kvlistValue", "kvlist_value") {
        return Value::Object(key_values(array(kv, "values", "values")));
    }
    // Bytes stay as their base64 text.
    get(v, "bytesValue", "bytes_value")
        .cloned()
        .unwrap_or(Value::Null)
}

fn key_values(items: &[Value]) -> Map<String, Value> {
    items
        .iter()
        .filter_map(|kv| {
            let key = kv.get("key")?.as_str()?;
            Some((
                key.to_owned(),
                kv.get("value").map_or(Value::Null, any_value),
            ))
        })
        .collect()
}

/// A map as JSON text; `None` when empty.
fn json_text(map: &Map<String, Value>) -> Option<String> {
    (!map.is_empty()).then(|| Value::Object(map.clone()).to_string())
}

fn text(v: Option<&Value>) -> Option<String> {
    v.and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
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
    let request: Value =
        serde_json::from_slice(body).map_err(|e| DatalakeError::MalformedPayload {
            reason: format!("OTLP logs request is not valid JSON: {e}"),
        })?;
    if !request.is_object() {
        return Err(DatalakeError::MalformedPayload {
            reason: "OTLP logs request must be a JSON object".into(),
        });
    }
    let mut groups: BTreeMap<Option<String>, Rows> = BTreeMap::new();
    for resource_logs in array(&request, "resourceLogs", "resource_logs") {
        let resource = key_values(array(
            resource_logs.get("resource").unwrap_or(&Value::Null),
            "attributes",
            "attributes",
        ));
        let resource_tenant = resource
            .get(tenant_attribute)
            .and_then(Value::as_str)
            .map(str::to_owned);
        let service = resource
            .get("service.name")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let resource_json = json_text(&resource);
        for scope_logs in array(resource_logs, "scopeLogs", "scope_logs") {
            let scope = scope_logs.get("scope").unwrap_or(&Value::Null);
            let (scope_name, scope_version) = (text(scope.get("name")), text(scope.get("version")));
            for log in array(scope_logs, "logRecords", "log_records") {
                let attributes = key_values(array(log, "attributes", "attributes"));
                let tenant = resource_tenant.clone().or_else(|| {
                    attributes
                        .get(tenant_attribute)
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                });
                let observed = time(get(log, "observedTimeUnixNano", "observed_time_unix_nano"));
                let body = get(log, "body", "body")
                    .map(any_value)
                    .and_then(|b| match b {
                        Value::Null => None,
                        Value::String(s) => Some(s),
                        other => Some(other.to_string()),
                    });
                let record = Record {
                    text: vec![
                        // The record's time, or when it was observed if unset.
                        time(get(log, "timeUnixNano", "time_unix_nano"))
                            .or_else(|| observed.clone()),
                        observed,
                        text(get(log, "severityText", "severity_text")),
                        body,
                        json_text(&attributes),
                        text(get(log, "traceId", "trace_id")).map(|s| s.to_ascii_lowercase()),
                        text(get(log, "spanId", "span_id")).map(|s| s.to_ascii_lowercase()),
                        text(get(log, "eventName", "event_name")),
                        service.clone(),
                        scope_name.clone(),
                        scope_version.clone(),
                        resource_json.clone(),
                    ],
                    ints: vec![
                        int(get(log, "severityNumber", "severity_number")),
                        int(get(log, "flags", "flags")),
                    ],
                };
                let rows = groups.entry(tenant).or_insert_with(Rows::new);
                for (builder, value) in rows.text.iter_mut().zip(record.text) {
                    builder.append_option(value);
                }
                for (builder, value) in rows.ints.iter_mut().zip(record.ints) {
                    builder.append_option(value);
                }
            }
        }
    }
    groups
        .into_iter()
        .map(|(tenant, rows)| {
            Ok(TenantLogs {
                tenant,
                batch: rows.finish()?,
            })
        })
        .collect()
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
            r#"{"ok":true,"order.id":42}"#
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
    fn invalid_payloads_are_rejected() {
        assert!(flatten_logs_json(b"not json", "tenant.id").is_err());
        assert!(flatten_logs_json(b"[1, 2]", "tenant.id").is_err());
        assert!(flatten_logs_json(b"{}", "tenant.id").unwrap().is_empty());
    }
}
